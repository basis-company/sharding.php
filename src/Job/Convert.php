<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Schema\Index;
use Basis\Sharding\Schema\Property;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Types\Type;
use Exception;

class Convert implements Job
{
    public function __construct(
        public readonly string $class,
    ) {
    }

    public function __invoke(Database $database)
    {
        $model = $database->schema->getModel($this->class);
        $buckets = $database->getBuckets($this->class);

        foreach ($buckets as $bucket) {
            $storage = $database->getStorage($bucket->storage);
            $table = $model->getTable($bucket, $storage);
            $driver = $storage->getDriver();
            if ($driver instanceof Tarantool) {
                $space = $driver->getMapper()->getSpace($table);
                if ($model->getFields() != $space->getFields()) {
                    $plan = [];
                    foreach ($model->getProperties() as $n => $property) {
                        if (in_array($property->name, $space->getFields())) {
                            $plan[$n] = [
                                'source' => array_search($property->name, $space->getFields()) + 1,
                            ];
                        } else {
                            $plan[$n] = [
                                'default' => $driver->getMapper()->converter->formatValue(
                                    type: $driver->getTarantoolType($property->type),
                                    value: $property->default ?: ''
                                ),
                            ];
                        }
                    }
                    $driver->query(<<<LUA
                        box.space[space]:format({})
                        box.space._vindex:pairs({box.space[space].id})
                            :filter(function(t) return t.iid > 0 end)
                            :each(function (t) box.space[space].index[t.name]:drop() end)
                        box.begin()
                        box.space[space]:pairs()
                            :each(function(t)
                                local tuple = {}
                                for n, cfg in pairs(plan) do
                                    if cfg.source ~= nil then
                                        tuple[n] = t[cfg.source]
                                    else
                                        tuple[n] = cfg.default
                                    end
                                end
                                box.space[space]:replace(tuple)
                            end)
                        box.commit()
                        box.space[space]:format(format)
                        for i, index in pairs(indexes) do
                            if box.space[space].index[index.name] ~= nil then
                                box.space[space]:create_index(index.name, {
                                    type = index.type,
                                    unique = index.unique,
                                    parts = index.fields
                                })
                            end
                        end
                    LUA, [
                        'space' => $table,
                        'plan' => $plan,
                        'format' => array_map(
                            fn (Property $property) => [
                                'name' => $property->name,
                                'type' => $driver->getTarantoolType($property->type),
                            ],
                            $model->getProperties(),
                        ),
                        'indexes' => array_map(
                            fn (Index $index) => [
                                'name' => $index->name,
                                'type' => 'tree',
                                'unique' => $index->unique,
                                'fields' => $index->fields
                            ],
                            $model->getIndexes(),
                        )
                    ]);
                }
            } elseif ($driver instanceof Doctrine) {
                $manager = $driver->getConnection()->createSchemaManager();
                $doctrineTable = $manager->introspectTable($table);
                $columns = [];
                $plan = [];
                $fields = array_map(fn ($column) => $column->getName(), $doctrineTable->getColumns());
                foreach ($model->getProperties() as $n => $property) {
                    if (!in_array($property->name, $fields)) {
                        $dbType = $driver->getDatabaseType($property->type);
                        $columns[] = new Column($property->name, Type::getType($dbType), ['notnull' => false]);
                        $plan[$property->name] = $driver->getDefaultValue($dbType);
                    }
                }
                if (count($columns)) {
                    $manager->alterTable(new TableDiff($doctrineTable, $columns));
                    foreach ($plan as $field => $default) {
                        $driver->query("update $table set $field = ?", [$default]);
                    }
                    $manager->alterTable(new TableDiff(
                        oldTable: $doctrineTable,
                        changedColumns: array_map(fn ($column) => new ColumnDiff($column, (clone $column)->setNotnull(true)), $columns),
                    ));
                }
            } elseif ($driver instanceof Runtime) {
                if (array_key_exists($table, $driver->data)) {
                    foreach ($driver->data[$table] as $i => $instance) {
                        foreach ($model->getProperties() as $property) {
                            if (!array_key_exists($property->name, $instance)) {
                                $instance[$property->name] = $driver->getDefaultValue($property->type);
                            }
                        }
                        $driver->data[$table][$i] = $instance;
                    }
                }
                continue;
            } else {
                throw new Exception("No driver convertation");
            }
        }
        $driver->syncSchema($database, $bucket);
    }
}