<?php

namespace Basis\Sharding\Driver;

use Basis\Sharding\Attribute\Autoincrement;
use Basis\Sharding\Database as ShardingDatabase;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Subscription;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Queryable;
use Basis\Sharding\Schema\Model;
use Basis\Sharding\Select;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Tools\DsnParser;
use Exception;
use ReflectionProperty;

class Doctrine implements Driver, Queryable
{
    public static int $x = 0;
    private ?Connection $connection = null;
    private $context = [];

    public function __construct(private readonly string $dsn)
    {
    }

    public function ackChanges(array $changes): void
    {
        $this->getConnection()->transactional(function () use ($changes) {
            foreach ($changes as $change) {
                $this->getConnection()->delete(Change::TABLE, ['id' => $change->id]);
            }
        });
    }

    public function create(string $table, array $data): object
    {
        return $this->insert($table, [$data])[0];
    }

    public function delete(string|object $table, array|int|null|string $id = null): ?object
    {
        return $this->getConnection()->transactional(function () use ($table, $id) {
            $result = $this->findOne($table, ['id' => $id]);
            if ($result) {
                $this->getConnection()->delete($table, ['id' => $id]);
                foreach ($this->getListeners($table) as $listener) {
                    $this->getConnection()->insert(Change::TABLE, [
                        'context' => json_encode(is_callable($this->context) ? call_user_func($this->context) : $this->context),
                        'listener' => $listener,
                        'tablename' => $table,
                        'action' => 'delete',
                        'data' => json_encode($result),
                    ]);
                }
            }
            return $result;
        });
    }

    public function dropTable(string $table): void
    {
        $this->getConnection()->createSchemaManager()->dropTable($table);
    }

    public function find(string $table, array $query = []): array
    {
        if (!$this->hasTable($table)) {
            return [];
        }
        $select = $this->getConnection()->createQueryBuilder()->select('*')->from($table);
        foreach ($query as $k => $v) {
            $select = $select->andWhere("$k = :$k")->setParameter($k, $v);
        }
        return array_map(fn($row) => (object) $row, $select->fetchAllAssociative());
    }

    public function findOne(string $table, array $query): ?object
    {
        if (!$this->hasTable($table)) {
            return null;
        }

        $select = $this->getConnection()->createQueryBuilder()->select('*')->from($table);
        foreach ($query as $k => $v) {
            $select = $select->andWhere("$k = :$k")->setParameter($k, $v);
        }

        $rows = $select->setMaxResults(1)->fetchAllAssociative();

        return count($rows) ? (object) $rows[0] : null;
    }

    public function findOrCreate(string $table, array $query, array $data = []): object
    {
        $row = $this->findOne($table, $query);
        if (!$row) {
            $row = $this->create($table, array_merge($query, $data));
        }
        return $row;
    }

    public function findOrFail(string $table, array $data): ?object
    {
        $row = $this->findOne($table, $data);
        if (!$row) {
            throw new Exception('No ' . $table . ' found');
        }
        return $row;
    }

    public function getChanges(string $listener = '', int $limit = 100): array
    {
        if (!$this->hasTable(Change::TABLE)) {
            return [];
        }

        $select = $this->getConnection()->createQueryBuilder()->select('*')->from(Change::TABLE);
        if ($listener) {
            $select = $select->where('listener = :listener')->setParameter('listener', $listener);
        }

        $changes = $select->fetchAllAssociative();
        foreach ($changes as $i => $change) {
            $changes[$i]['context'] = json_decode($change['context'], true);
            $changes[$i]['data'] = json_decode($change['data'], true);
        }

        return array_map(fn($change) => new Change(...array_values($change)), $changes);
    }

    public function getConnection(): Connection
    {
        if ($this->connection === null) {
            $params = (new DsnParser())->parse($this->dsn);
            $this->connection = DriverManager::getConnection($params);
        }

        return $this->connection;
    }

    public function getDatabaseType(string $type): string
    {
        return match ($type) {
            'array' => 'json',
            'int' => 'integer',
            default => $type,
        };
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
        return $this->dsn;
    }

    public function getListeners(string $table): array
    {
        $listeners = [];
        foreach (['*', $table] as $filter) {
            foreach ($this->find(Subscription::TABLE, ['tablename' => $filter]) as $subscription) {
                $listeners[] = $subscription->listener;
            }
        }

        return array_values(array_unique($listeners));
    }

    public function getUsage(): int
    {
        $platform = $this->getConnection()->getDatabasePlatform();
        $data = null;
        if ($platform instanceof MySQLPlatform) {
            $data = $this->getConnection()
                ->createQueryBuilder()
                ->select("table_name name, data_length + index_length usage")
                ->from('information_schema.tables')
                ->fetchAllAssociative();
        }

        if ($platform instanceof PostgreSQLPlatform) {
            $data = $this->getConnection()
                ->createQueryBuilder()
                ->select("datname name, pg_database_size(datname) usage")
                ->from('pg_database')
                ->fetchAllAssociative();
        }

        if ($data !== null) {
            return array_sum(array_map(fn($row) => $row['usage'], $data));
        }

        throw new Exception('Not implement usage for: ' . get_class($this->getConnection()->getDatabasePlatform()));
    }

    public function hasTable(string $table): bool
    {
        return $this->getConnection()->createSchemaManager()->tableExists($table);
    }

    public function insert(string $table, array $rows): array
    {
        return $this->getConnection()->transactional(function () use ($table, $rows) {
            $listeners = $this->getListeners($table);
            $result = [];
            foreach ($rows as $row) {
                $row = $this->prepare($table, $row);
                $this->getConnection()->insert($table, $row);
                $result[] = (object) $row;
                foreach ($listeners as $listener) {
                    $this->getConnection()->insert(Change::TABLE, [
                        'context' => json_encode(is_callable($this->context) ? call_user_func($this->context) : $this->context),
                        'listener' => $listener,
                        'tablename' => $table,
                        'action' => 'create',
                        'data' => json_encode($row),
                    ]);
                }
            }

            return $result;
        });
    }

    public function prepare(string $table, array $data): array
    {
        $platform = $this->getConnection()->getDatabasePlatform();
        $sorted = [];
        foreach ($this->getConnection()->createSchemaManager()->introspectSchema()->getTable($table)->getColumns() as $column) {
            if (array_key_exists($column->getName(), $data)) {
                $sorted[$column->getName()] = $data[$column->getName()];
            } else {
                $sorted[$column->getName()] = $column->getType()->convertToPHPValue('', $platform);
            }
            if ($column->getType() === 'array') {
                $sorted[$column->getName()] = $sorted[$column->getName()];
            }
        }
        return $sorted;
    }
    public function query(string $query, array $params = []): array|object|null
    {
        return $this->getConnection()->executeQuery($query, $params)->fetchAllAssociative();
    }

    public function registerChanges(string $table, string $listener): void
    {
        if (!$this->hasTable(Subscription::TABLE)) {
            $this->syncModel(new Model('', Change::TABLE, Change::class));
            $this->syncModel(new Model('', Subscription::TABLE, Subscription::class));
        }

        $this->getConnection()->insert(Subscription::TABLE, [
            'listener' => $listener,
            'tablename' => $table,
        ]);
    }

    public function reset(): self
    {
        foreach ($this->getConnection()->createSchemaManager()->listTables() as $table) {
            $this->dropTable($table->getName());
        }

        return $this;
    }

    public function select(string $table): Select
    {
        return new Select(function (Select $select) use ($table) {
            $query = $this->getConnection()->createQueryBuilder()->select('*')->from($table);
            foreach ($select->conditions as $field => $where) {
                foreach ($where->getConditions() as $condition) {
                    if ($condition->isGreaterThan !== null) {
                        $query->andWhere($field . ' > :' . $field);
                        $query->setParameter($field, $condition->isGreaterThan);
                    }
                    if ($condition->isLessThan !== null) {
                        $query->andWhere($field . ' < :' . $field);
                        $query->setParameter($field, $condition->isLessThan);
                    }
                    if ($condition->equals !== null) {
                        $query->andWhere($field . ' = :' . $field);
                        $query->setParameter($field, $condition->equals);
                    }
                }
            }

            if ($select->orderBy) {
                $query->orderBy($select->orderBy, $select->orderByAscending ? 'asc' : 'desc');
            }

            return array_map(fn ($row) => (object) $row, $query->setMaxResults($select->limit)->fetchAllAssociative());
        });
    }

    public function setContext(array|callable $context): void
    {
        $this->context = $context;
    }

    public function syncModel(Model $model, ?Bucket $bucket = null, ?Storage $storage = null)
    {
        $name = $model->getTable($bucket, $storage);
        $manager = $this->getConnection()->createSchemaManager();
        $schema = $manager->introspectSchema();
        $table = $schema->hasTable($name) ? $schema->getTable($name) : $schema->createTable($name);

        foreach ($model->getProperties() as $property) {
            if ($table->hasColumn($property->name)) {
                continue;
            }
            $column = $table->addColumn($property->name, match ($property->type) {
                'float' => 'float',
                '?float' => 'float',
                'string' => 'text',
                '?string' => 'text',
                'array' => 'json',
                '?array' => 'json',
                default => 'integer'
            });
            $column->setNotnull(true);

            if ($model->class && $property->name == 'id') {
                $reflection = new ReflectionProperty($model->class, $property->name);
                if (count($reflection->getAttributes(Autoincrement::class))) {
                    $column->setAutoincrement(true);
                }
            }
        }

        foreach ($model->getIndexes() as $index) {
            if ($table->hasIndex($name . '_' . $index->name)) {
                continue;
            }
            if ($index->unique) {
                $table->addUniqueIndex($index->fields, $name . '_' . $index->name);
            } else {
                $table->addIndex($index->fields, $name . '_' . $index->name);
            }
        }

        $this->getConnection()->createSchemaManager()->migrateSchema($schema);
    }

    public function syncSchema(ShardingDatabase $shardingDatabase, Bucket $bucket): void
    {
        $bootstrappers = [];
        $storage = $bucket->isCore() ? null : $shardingDatabase->getStorage($bucket->storage);

        foreach ($shardingDatabase->schema->getModels($bucket->name) as $model) {
            if ($model->class && is_a($model->class, Bootstrap::class, true)) {
                if (!$this->hasTable($model->getTable($bucket))) {
                    $bootstrappers[] = $model->class;
                }
            }
            $this->syncModel($model, $bucket, $storage);
        }

        foreach ($bootstrappers as $bootstrapper) {
            $bootstrapper::bootstrap($shardingDatabase);
        }
    }

    public function update(string|object $table, array|int|string $id, ?array $data = null): ?object
    {
        return $this->getConnection()->transactional(function () use ($table, $id, $data) {
            $this->getConnection()->update($table, $data, ['id' => $id]);
            $row = $this->findOne($table, ['id' => $id]);
            foreach ($this->getListeners($table) as $listener) {
                $this->getConnection()->insert(Change::TABLE, [
                    'context' => json_encode(is_callable($this->context) ? call_user_func($this->context) : $this->context),
                    'listener' => $listener,
                    'tablename' => $table,
                    'action' => 'update',
                    'data' => json_encode($row),
                ]);
            }
            return $row;
        });
    }
}
