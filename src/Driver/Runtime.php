<?php

namespace Basis\Sharding\Driver;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Subscription;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Schema\Model;
use Basis\Sharding\Select;
use Exception;

class Runtime implements Driver
{
    public array $data = [];
    public array $models = [];
    private $context = [];

    public function ackChanges(array $changes): void
    {
        foreach ($changes as $change) {
            $this->delete(Change::TABLE, $change->id);
        }
    }

    public function create(string $class, array $data): object
    {
        return $this->insert($class, [$data])[0];
    }

    public function delete(string|object $class, array|int|null|string $id = null): ?object
    {
        foreach ($this->data[$class] as $i => $row) {
            if ($row['id'] == $id) {
                unset($this->data[$class][$i]);
                $this->data[$class] = array_values($this->data[$class]);
                $this->registerChange($class, 'delete', $row);
                return (object) $row;
            }
        }

        return null;
    }

    public function dropTable(string $table): void
    {
        unset($this->data[$table]);
    }

    public function find(string $class, array $query = []): array
    {
        if (!array_key_exists($class, $this->data)) {
            $this->data[$class] = [];
        }
        if (count($query)) {
            $data = [];
            foreach ($this->data[$class] as $row) {
                if (!array_diff_assoc($query, $row)) {
                    $data[] = $row;
                }
            }
        } else {
            $data = $this->data[$class];
        }

        if (count($data) && array_key_exists(0, $data[0])) {
            usort($data, fn ($a, $b) => $a['id'] <=> $b['id']);
        }
        return $data;
    }

    public function findOne(string $class, array $query): ?object
    {
        $rows = $this->find($class, $query);
        return count($rows) ? (object) $rows[0] : null;
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        $instance = $this->findOne($class, $query);
        if (!$instance) {
            $instance = $this->create($class, array_merge($query, $data));
        }
        return (object) $instance;
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
        $changes = [];
        if (array_key_exists(Change::TABLE, $this->data)) {
            foreach ($this->data[Change::TABLE] as $change) {
                if (!$listener || $change['listener'] == $listener) {
                    $changes[] = new Change(...array_values($change));
                    if (count($changes) >= $limit) {
                        break;
                    }
                }
            }
        }
        return $changes;
    }

    public function getDefaultValue(string $type)
    {
        switch ($type) {
            case 'int':
                return 0;
            case 'string':
                return '';
            case 'bool':
                return false;
        }
    }

    public function getDsn(): string
    {
        return '';
    }

    public function getListeners(string $table): array
    {
        if (strpos($table, 'sharding_') === 0) {
            return [];
        }

        $listeners = [];
        if (array_key_exists(Subscription::TABLE, $this->data)) {
            foreach ($this->data[Subscription::TABLE] as $subscription) {
                if (in_array($subscription['table'], [$table, '*'])) {
                    $listeners[$subscription['listener']] = true;
                }
            }
        }

        return array_keys($listeners);
    }

    public function getUsage(): int
    {
        return strlen(json_encode($this->data));
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, $this->data);
    }

    public function insert(string $table, array $rows): array
    {
        if (array_key_exists($table, $this->models)) {
            foreach ($rows as $i => $row) {
                $sorted = [];
                foreach ($this->models[$table]->getProperties() as $property) {
                    if (array_key_exists($property->name, $row)) {
                        $sorted[$property->name] = $row[$property->name];
                    } else {
                        $sorted[$property->name] = $this->getDefaultValue($property->type);
                    }
                }
                $rows[$i] = $sorted;
            }
        }

        foreach ($rows as $row) {
            $this->data[$table][] = $row;
            $this->registerChange($table, 'create', $row);
        }

        return array_map(fn ($row) => (object) $row, $rows);
    }

    public function query(string $query, array $params = []): array
    {
        throw new Exception('Not implemented');
    }

    public function registerChange(string $table, string $action, array $data): void
    {
        if (strpos($table, 'sharding_') === 0) {
            return;
        }
        if (array_key_exists(Subscription::TABLE, $this->data)) {
            $listeners = [];
            foreach ($this->data[Subscription::TABLE] as $subscription) {
                if (in_array($subscription['table'], [$table, '*'])) {
                    $listeners[$subscription['listener']] = true;
                }
            }
            foreach (array_keys($listeners) as $listener) {
                if (!array_key_exists(Change::TABLE, $this->data)) {
                    $model = new Model(Change::class, Change::TABLE);
                    $this->data[$model->table] = [];
                    $this->models[$model->table] = $model;
                }
                $this->data[Change::TABLE][] = [
                    'id' => count($this->data[Change::TABLE]) + 1,
                    'listener' => $listener,
                    'table' => $table,
                    'action' => $action,
                    'data' => $data,
                    'context' => is_callable($this->context) ? call_user_func($this->context) : $this->context,
                ];
            }
        }
    }

    public function registerChanges(string $table, string $listener): void
    {
        if (!$this->hasTable(Subscription::TABLE)) {
            $model = new Model(Subscription::class, Subscription::TABLE);
            $this->data[$model->table] = [];
            $this->models[$model->table] = $model;
        }

        $this->data[Subscription::TABLE][] = [
            'id' => count($this->data[Subscription::TABLE]) + 1,
            'listener' => $listener,
            'table' => $table,
        ];
    }

    public function reset(): self
    {
        $this->data = [];
        $this->models = [];

        return $this;
    }

    public function select(string $table): Select
    {
        return new Select(function (Select $select) use ($table) {
            $result = [];
            $rows = $this->find($table);
            if ($select->orderBy) {
                usort($rows, function ($a, $b) use ($select) {
                    return ($select->orderByAscending ? 1 : -1 ) * ($a[$select->orderBy] <=> $b[$select->orderBy]);
                });
            }
            foreach ($rows as $row) {
                foreach ($select->conditions as $field => $where) {
                    foreach ($where->getConditions() as $condition) {
                        if ($condition->isGreaterThan !== null && $row[$field] <= $condition->isGreaterThan) {
                            continue 3;
                        }
                        if ($condition->isLessThan !== null && $row[$field] >= $condition->isLessThan) {
                            continue 3;
                        }
                        if ($condition->equals !== null && $row[$field] != $condition->equals) {
                            continue 3;
                        }
                    }
                }
                $result[] = (object) $row;
                if ($select->limit && count($result) == $select->limit) {
                    break;
                }
            }

            return $result;
        });
    }

    public function setContext(array|callable $context): void
    {
        $this->context = $context;
    }

    public function syncSchema(Database $database, Bucket $bucket): void
    {
        $bootstrappers = [];
        $storage = $bucket->isCore() ? null : $database->getStorage($bucket->storage);

        foreach ($database->schema->getSegmentByName($bucket->name, create: false)->getModels() as $model) {
            $table = $model->getTable($bucket, $storage);
            if (array_key_exists($table, $this->data)) {
                continue;
            }
            $this->data[$table] = [];
            $this->models[$table] = $model;
            if (is_a($model->class, Bootstrap::class, true)) {
                $bootstrappers[] = $model->class;
            }
        }

        foreach ($bootstrappers as $bootstrapper) {
            $bootstrapper::bootstrap($database);
        }
    }

    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        if (!array_key_exists($class, $this->data)) {
            return null;
        }
        foreach ($this->data[$class] as $i => $row) {
            if ($row['id'] == $id) {
                $this->data[$class][$i] = array_merge($row, $data);
                $this->registerChange($class, 'update', $this->data[$class][$i]);
                return (object) $this->data[$class][$i];
            }
        }

        return null;
    }
}
