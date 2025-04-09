<?php

namespace Basis\Sharded\Driver;

use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Router;
use Basis\Sharded\Schema\Schema;
use Exception;

class Runtime implements Driver
{
    public static array $data = [];
    public static array $models = [];

    public function create(string $class, array $data): object
    {
        $sorted = ['bucket' => $data['bucket']];
        foreach (self::$models[$class]->getProperties() as $property) {
            if (array_key_exists($property->name, $data)) {
                $sorted[$property->name] = $data[$property->name];
            } else {
                $sorted[$property->name] = $this->getDefaultPropetryValue($property->type);
            }
        }
        self::$data[$class][] = $sorted;
        return (object) $sorted;
    }

    public function getDefaultPropetryValue(string $type)
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

    public function find(string $class, array $query = []): array
    {
        if (!array_key_exists($class, self::$data)) {
            throw new Exception("No $class");
        }
        if (count($query)) {
            $data = [];
            foreach (self::$data[$class] as $row) {
                if (!array_diff_assoc($query, $row)) {
                    $data[] = $row;
                }
            }
        } else {
            $data = self::$data[$class];
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
        if (!$this->findOne($class, $query)) {
            $data = $this->create($class, $data);
        }
        return (object) $data;
    }

    public function get(string $class, int $id): object
    {
        return $this->find($class, ['id' => $id])[0];
    }

    public function getDsn(): string
    {
        return '';
    }

    public function update(string $class, int $id, array $data): void
    {
        foreach (self::$data[$class] as $i => $row) {
            if ($row['id'] == $id) {
                self::$data[$class][$i] = array_merge($row, $data);
                break;
            }
        }
    }

    public function syncSchema(Schema $schema, Router $router): void
    {
        $bootstrappers = [];

        foreach ($schema->models as $model) {
            if (array_key_exists($model->table, self::$data)) {
                continue;
            }
            self::$data[$model->table] = [];
            self::$models[$model->table] = $model;
            if (is_a($model->class, Bootstrap::class, true)) {
                $bootstrappers[] = $model->class;
            }
        }

        foreach ($bootstrappers as $bootstrapper) {
            $bootstrapper::bootstrap($router);
        }
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, self::$data);
    }
}
