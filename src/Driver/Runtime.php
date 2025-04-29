<?php

namespace Basis\Sharded\Driver;

use Basis\Sharded\Database;
use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Driver;
use Exception;

class Runtime implements Driver
{
    public array $data = [];
    public array $models = [];

    public function create(string $class, array $data): object
    {
        foreach ($this->models[$class]->getProperties() as $property) {
            if (array_key_exists($property->name, $data)) {
                $sorted[$property->name] = $data[$property->name];
            } else {
                $sorted[$property->name] = $this->getDefaultPropetryValue($property->type);
            }
        }
        $this->data[$class][] = $sorted;
        return (object) $sorted;
    }

    public function delete(string|object $class, array|int|null|string $id = null): ?object
    {
        foreach ($this->data[$class] as $i => $row) {
            if ($row['id'] == $id) {
                unset($this->data[$class][$i]);
                $this->data[$class] = array_values($this->data[$class]);
                return (object) $row;
            }
        }

        return null;
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

    public function getUsage(): int
    {
        return strlen(json_encode($this->data));
    }

    public function find(string $class, array $query = []): array
    {
        if (!array_key_exists($class, $this->data)) {
            throw new Exception("No $class");
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

    public function getDsn(): string
    {
        return '';
    }

    public function reset(): self
    {
        $this->data = [];
        $this->models = [];

        return $this;
    }

    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        if (!array_key_exists($class, $this->data)) {
            return null;
        }
        foreach ($this->data[$class] as $i => $row) {
            if ($row['id'] == $id) {
                $this->data[$class][$i] = array_merge($row, $data);
                return (object) $this->data[$class][$i];
            }
        }

        return null;
    }

    public function syncSchema(Database $database, string $segment): void
    {
        $bootstrappers = [];

        foreach ($database->schema->getSegmentByName($segment, create: false)->getModels() as $model) {
            if (array_key_exists($model->table, $this->data)) {
                continue;
            }
            $this->data[$model->table] = [];
            $this->models[$model->table] = $model;
            if (is_a($model->class, Bootstrap::class, true)) {
                $bootstrappers[] = $model->class;
            }
        }

        foreach ($bootstrappers as $bootstrapper) {
            $bootstrapper::bootstrap($database);
        }
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, $this->data);
    }
}
