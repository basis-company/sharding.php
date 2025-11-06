<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Job;
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
                    LUA, [
                        'space' => $table,
                        'plan' => $plan,
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
                continue;
            } else {
                throw new Exception("No driver convertation");
            }
            $driver->syncSchema($database, $bucket);
        }
    }
}