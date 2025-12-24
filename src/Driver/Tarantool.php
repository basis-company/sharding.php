<?php

namespace Basis\Sharding\Driver;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Subscription;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Queryable;
use Basis\Sharding\Schema\Model;
use Basis\Sharding\Select;
use Exception;
use Tarantool\Client\Client;
use Tarantool\Client\Schema\Criteria;
use Tarantool\Client\Schema\Operations;
use Tarantool\Mapper\Mapper;

class Tarantool implements Driver, Queryable
{
    public readonly Mapper $mapper;
    private $context = [];

    public function __construct(
        protected readonly string $dsn,
    ) {
        $this->mapper = new Mapper(Client::fromDsn($dsn));
    }

    public function ackChanges(array $changes): void
    {
        array_map(fn ($change) => $this->mapper->delete(Change::TABLE, $change), $changes);
    }

    public function create(string $table, array $data): object
    {
        $listeners = $this->getListeners($table);
        if (!count($listeners)) {
            return $this->mapper->create($table, $data);
        }

        return $this->processLuaResult(
            table: $table,
            action: 'create',
            params: $this->mapper->getSpace($table)->getTuple($data),
            query: 'local tuple = box.space[table]:insert(params)',
        );
    }

    public function delete(string|object $table, array|int|null|string $id = null): ?object
    {
        $listeners = $this->getListeners($table);
        if (!count($listeners)) {
            return $this->mapper->delete($table, is_array($id) ? $id : ['id' => $id]);
        }

        return $this->processLuaResult(
            table: $table,
            action: 'delete',
            params: is_array($id) ? [$id['id']] : [$id],
            query: 'local tuple = box.space[table]:delete(params)',
        );
    }

    public function dropTable(string $table): void
    {
        $this->mapper->getSpace($table)->drop();
    }

    public function find(string $table, array $query = []): array
    {
        return $this->mapper->find($table, $query);
    }

    public function findOne(string $table, array $query): ?object
    {
        return $this->mapper->findOne($table, $query);
    }

    public function findOrCreate(string $table, array $query, array $data = []): object
    {
        if (!count($this->getListeners($table))) {
            return $this->mapper->findOrCreate($table, $query, $data);
        }

        $index = $this->mapper->getSpace($table)->castIndex(array_keys($query));
        $select = [];
        foreach ($index['fields'] as $field) {
            if (array_key_exists($field, $query)) {
                $select[] = $query[$field];
            } else {
                break;
            }
        }

        return $this->processLuaResult(
            table: $table,
            action: 'create',
            params: [
                $index['iid'],
                $select,
                $this->mapper->getSpace($table)->getTuple($data)
            ],
            query: <<<QUERY
                local tuples = box.space[table].index[params[1]]:select(params[2], {limit=1})
                local tuple = tuples[1]
                if tuple == nil then
                    tuple = box.space[table]:insert(params[3])
                else
                    register_changes = false
                end
            QUERY,
        );
    }

    public function findOrFail(string $table, array $query): ?object
    {
        $row = $this->findOne($table, $query);
        if (!$row) {
            throw new Exception('No ' . $table . ' found');
        }
        return $row;
    }

    public function getChanges(string $listener = '', int $limit = 100): array
    {
        if (!$this->hasTable(Subscription::TABLE)) {
            return [];
        }
        if ($listener) {
            $criteria = Criteria::index('listener')->andKey([$listener]);
        } else {
            $criteria = Criteria::allIterator();
        }

        return $this->mapper->find(Change::TABLE, $criteria->andLimit($limit));
    }

    public function getDsn(): string
    {
        return $this->dsn;
    }

    public function getListeners(string $table): array
    {
        if (!$this->hasTable(Subscription::TABLE)) {
            return [];
        }

        $listeners = [];
        foreach ($this->find(Subscription::TABLE) as $subscription) {
            if (in_array($subscription->tablename, [$table, '*'])) {
                $listeners[$subscription->listener] = $subscription->listener;
            }
        }

        return array_values($listeners);
    }

    public function getMapper(): Mapper
    {
        return $this->mapper;
    }

    public function getTarantoolType(string $type): string
    {
        return match ($type) {
            'int' => 'unsigned',
            'string' => 'string',
            'array' => '*',
            default => throw new Exception('Invalid type'),
        };
    }

    public function getUsage(): int
    {
        return $this->mapper->evaluate("return box.slab.info().items_size")[0];
    }

    public function hasListeners(string $table): bool
    {
        return count($this->find(Subscription::TABLE, ['table' => $table])) > 0;
    }

    public function hasTable(string $table): bool
    {
        return $this->mapper->hasSpace($table);
    }

    public function insert(string $table, array $rows): array
    {
        return array_map(fn ($row) => $this->create($table, $row), $rows);
    }

    public function processLuaResult(string $table, array $params, string $query, string $action): object
    {
        $listeners = $this->getListeners($table);
        $context = is_callable($this->context) ? call_user_func($this->context) : $this->context;

        [$result] = $this->mapper->call(
            <<<LUA
                box.begin()
                local register_changes = true
                {$query}
                if box.sequence.sharding_change == nil then
                    box.schema.sequence.create(sharding_change)
                end
                if register_changes then
                    for i, listener in pairs(listeners) do
                        box.space.sharding_change:insert({
                            box.sequence.sharding_change:next(),
                            listener,
                            table,
                            action,
                            tuple:tomap({names_only = true }),
                            context
                        })
                    end
                end
                box.commit()
                return tuple
            LUA,
            compact('table', 'action', 'params', 'listeners', 'context')
        );

        return (object) $this->mapper->getSpace($table)->getInstance($result);
    }

    public function query(string $query, array $params = []): array|object|null
    {
        return $this->mapper->evaluate($query, $params);
    }

    public function registerChanges(string $table, string $listener): void
    {
        if (!$this->hasTable(Subscription::TABLE)) {
            $this->syncModel(new Model('', Change::TABLE, Change::class));
            $this->syncModel(new Model('', Subscription::TABLE, Subscription::class));
        }

        $this->mapper->create(Subscription::TABLE, [
            'listener' => $listener,
            'tablename' => $table,
        ]);
    }

    public function reset(): self
    {
        $this->mapper->dropUserSpaces();
        return $this;
    }

    public function select(string $table): Select
    {
        return new Select(function (Select $select) use ($table) {
            $fields = array_keys($select->conditions);
            $index = $this->mapper->getSpace($table)->castIndex($fields);
            $criteria = Criteria::index($index['iid']);
            if ($select->limit) {
                $criteria = $criteria->andLimit($select->limit);
            }
            if (count($select->conditions) > 1) {
                throw new Exception("Not implemented");
            }
            foreach ($select->conditions as $where) {
                $conditions = $where->getConditions();
                if (count($conditions) > 1) {
                    throw new Exception("Not implemented");
                }
                [$condition] = $conditions;
                if ($condition->isGreaterThan !== null) {
                    $criteria = $criteria->andGtIterator()->andKey([$condition->isGreaterThan]);
                }
                if ($condition->isLessThan !== null) {
                    $criteria = $criteria->andLtIterator()->andKey([$condition->isLessThan]);
                    if (count($fields) != 1 || $select->orderBy !== $fields[0] || $select->orderByAscending) {
                        throw new Exception("Unordered query not supported");
                    }
                }
                if ($condition->equals !== null) {
                    $criteria = $criteria->andEqIterator()->andKey([$condition->equals]);
                }
            }

            $tuples = $this->mapper->client->getSpace($table)->select($criteria);
            return array_map(fn ($tuple) => $this->mapper->getSpace($table)->getInstance($tuple), $tuples);
        });
    }

    public function setContext(array|callable $context): void
    {
        $this->context = $context;
    }

    public function syncModel(Model $model, ?Bucket $bucket = null, ?Storage $storage = null)
    {
        try {
            $this->mapper->evaluate("box.session.su('admin')");
        } catch (Exception) {
        }

        $table = $model->getTable($bucket, $storage);

        $present = $this->mapper->hasSpace($table);
        if ($present) {
            $space = $this->mapper->getSpace($table);
        } else {
            $space = $this->mapper->createSpace($table, [
                'if_not_exists' => true,
            ]);
        }

        foreach ($model->getProperties() as $property) {
            if (in_array($property->name, $space->getFields())) {
                continue;
            }
            $space->addProperty($property->name, $this->getTarantoolType($property->type));
        }

        foreach ($model->getIndexes() as $index) {
            $space->addIndex($index->fields, [
                'if_not_exists' => true,
                'name' => $index->name,
                'unique' => $index->unique,
                'type' => $index->type,
            ]);
        }
    }

    public function syncSchema(Database $database, Bucket $bucket): void
    {
        $bootstrap = [];
        $storage = $bucket->isCore() ? null : $database->getStorage($bucket->storage);

        foreach ($database->schema->getModels($bucket->name) as $model) {
            if (!$this->mapper->hasSpace($model->getTable($bucket)) && is_a($model->class, Bootstrap::class, true)) {
                $bootstrap[] = $model->class;
            }
            $this->syncModel($model, $bucket, $storage);
        }

        foreach ($bootstrap as $class) {
            $class::bootstrap($database);
        }
    }

    public function update(string|object $table, array|int|string $id, ?array $data = null): ?object
    {
        if (!count($this->getListeners($table))) {
            $operations = null;
            foreach ($data as $k => $v) {
                if ($operations instanceof Operations) {
                    $operations = $operations->andSet($k, $v);
                } else {
                    $operations = Operations::set($k, $v);
                }
            }

            $changes = $this->mapper->client->getSpace($table)->update([$id], $operations);
            return count($changes) ? (object) $changes[0] : null;
        }

        $operations = [];
        foreach ($data as $k => $v) {
            $operations[] = ['=', $k, $v];
        }

        return $this->processLuaResult(
            table: $table,
            action: 'update',
            params: [
                $id,
                $operations,
            ],
            query: 'local tuple = box.space[table]:update(params[1], params[2])',
        );
    }
}
