<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Schema\Index;
use Basis\Sharding\Schema\Property;
use Exception;
use ReflectionProperty;

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
                $modelIndexes = array_map(
                    fn (Index $index) => [
                        'name' => $index->name,
                        'type' => 'tree',
                        'unique' => $index->unique,
                        'fields' => $index->fields
                    ],
                    $model->getIndexes(),
                );
                $spaceIndexes = array_map(
                    fn ($index) => [
                        'name' => $index['name'],
                        'type' => 'tree',
                        'unique' => $index['opts']['unique'],
                        'fields' => $index['fields']
                    ],
                    (new ReflectionProperty($space::class, 'indexes'))->getValue($space),
                );
                if ($model->getFields() != $space->getFields() || $modelIndexes != $spaceIndexes) {
                    $plan = [];
                    foreach ($model->getProperties() as $n => $property) {
                        $default = $driver->getMapper()->converter->formatValue(
                            type: $driver->getTarantoolType($property->type),
                            value: $property->default ?: ''
                        );
                        if (in_array($property->name, $space->getFields())) {
                            $plan[$n] = [
                                'source' => array_search($property->name, $space->getFields()) + 1,
                                'default' => $default,
                            ];
                        } else {
                            $plan[$n] = [
                                'default' => $default,
                            ];
                        }
                    }
                    $driver->query(<<<LUA
                        box.begin()
                        local space_old = box.space[space]
                        local old_field_positions = {}
                        local format_old = space_old:format()
                        for pos, field in pairs(format_old) do
                            old_field_positions[field.name] = pos
                        end
                        for i, field in pairs(format) do
                            local old_pos = old_field_positions[field.name]
                            if old_pos ~= nil then
                                if format_old[old_pos]['is_nullable'] ~= nil then
                                    format[i]['is_nullable'] = format_old[old_pos]['is_nullable']
                                end
                                if format_old[old_pos]['reference'] ~= nil then
                                    format[i]['reference'] = format_old[old_pos]['reference']
                                end
                            end
                        end

                        local space_new = box.schema.space.create('temp_' .. space)
                        space_new:format(format)
                        for i, index in pairs(indexes) do
                            space_new:create_index(index.name, {
                                type = index.type,
                                unique = index.unique,
                                parts = index.fields
                            })
                        end
                        space_old:pairs()
                            :each(function(t)
                                local tuple = {}
                                for n, cfg in pairs(plan) do
                                    if cfg.source ~= nil and t[cfg.source] ~= nil then
                                        tuple[n] = t[cfg.source]
                                    else
                                        tuple[n] = cfg.default
                                    end
                                end
                                space_new:insert(tuple)
                            end)
                        space_old:drop()
                        space_new:rename(space)
                        box.commit()
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
                        'indexes' => $modelIndexes,
                    ]);
                }
            } elseif ($driver instanceof Doctrine) {
                $manager = $driver->getConnection()->createSchemaManager();
                $doctrineTable = $manager->introspectTable($table);
                $tableIndexes = array_keys($manager->listTableIndexes($table));
                $modelIndexes = array_map(fn ($index) => $table . '_' . $index->name, $model->getIndexes());
                $columns = [];
                $fields = array_map(fn ($column) => $column->getName(), $doctrineTable->getColumns());
                $modelFields = array_map(fn ($property) => $property->name, $model->getProperties());
                if ($fields !== $modelFields || $tableIndexes !== $modelIndexes) {
                    $connection = $driver->getConnection();
                    $connection->beginTransaction();

                    try {
                        $tempTable = 'temp_' . $table;
                        $createSQL = "CREATE TABLE $tempTable (\n";
                        $columns = [];

                        foreach ($model->getProperties() as $property) {
                            $type = match ($property->type) {
                                'int', 'integer' => 'INTEGER',
                                'string', 'text' => 'TEXT',
                                'float', 'double' => 'REAL',
                                'bool', 'boolean' => 'BOOLEAN',
                                'datetime' => 'TIMESTAMP',
                                'date' => 'DATE',
                                'json' => 'JSONB',
                                default => 'TEXT'
                            };
                            $colDef = $property->name . " $type";
                            $columns[] = $colDef;
                        }

                        $createSQL .= implode(",\n", $columns) . "\n)";
                        $connection->executeStatement($createSQL);
                        $selectFields = [];

                        foreach ($model->getProperties() as $property) {
                            $dbType = $driver->getDatabaseType($property->type);
                            $default_value = $driver->getDefaultValue($dbType);
                            $default_value = $dbType == 'string' ? $connection->quote('') : $default_value;
                            if (in_array($property->name, $fields)) {
                                $selectFields[] = $property->name;
                            } else {
                                $selectFields[] = $default_value;
                            }
                        }

                        $copySQL = "INSERT INTO $tempTable 
                            SELECT " . implode(", ", $selectFields) . "
                            FROM $table";
                        $connection->executeStatement($copySQL);

                        foreach ($tableIndexes as $index) {
                            $connection->executeStatement("DROP INDEX {$index}");
                        }

                        //it won't work if other tables have foreign keys that reference this table
                        $connection->executeStatement("DROP TABLE $table");
                        $connection->executeStatement("ALTER TABLE $tempTable RENAME TO $table");

                        foreach ($model->getIndexes() as $index) {
                            $indexName = $table . '_' . implode('_', $index->fields);
                            $unique = $index->unique ? 'UNIQUE ' : '';
                            $fields = implode(', ', $index->fields);
                            $connection->executeStatement(
                                "CREATE $unique INDEX $indexName ON $table ($fields)"
                            );
                        }
                        $connection->commit();
                    } catch (Exception $e) {
                        $connection->rollBack();
                        throw $e;
                    }
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
                $modelFields = array_map(fn ($property) => $property->name, $model->getProperties());
                $tableFields = count($driver->data[$table]) ? array_keys($driver->data[$table][0]) : null;
                if ($modelFields != $tableFields) {
                    $tempTable = [];
                    foreach ($driver->data[$table] as $num => $instance) {
                        foreach ($modelFields as $k => $v) {
                            $tempTable[$num][$v] = $instance[$v];
                        }
                    }
                    $driver->data[$table] = $tempTable;
                    unset($tempTable);
                }
                continue;
            } else {
                throw new Exception("No driver convertation");
            }
        }
        $driver->syncSchema($database, $bucket);
    }
}
