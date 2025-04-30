<?php

namespace Basis\Sharding\Driver;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Subscription;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Tracker;
use Basis\Sharding\Schema\Model;
use Exception;

class Runtime implements Driver, Tracker
{
    public array $data = [];
    public array $models = [];
    public array $context = [];

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
        $this->registerChange($class, 'create', $sorted);
        return (object) $sorted;
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
                $this->registerChange($class, 'update', $this->data[$class][$i]);
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

    public function setContext(array $context): void
    {
        $this->context = $context;
    }

    public function track(string $table, string $listener): void
    {
        if (!$this->hasTable(Subscription::getSpaceName())) {
            $model = new Model(Subscription::class, Subscription::getSpaceName());
            $this->data[$model->table] = [];
            $this->models[$model->table] = $model;
        }

        $this->data[Subscription::getSpaceName()][] = [
            'id' => count($this->data[Subscription::getSpaceName()]) + 1,
            'listener' => $listener,
            'table' => $table,
        ];
    }

    public function registerChange(string $table, string $action, array $data): void
    {
        if (array_key_exists(Subscription::getSpaceName(), $this->data)) {
            foreach ($this->data[Subscription::getSpaceName()] as $subscription) {
                if ($subscription['table'] == $table) {
                    if (!array_key_exists(Change::getSpaceName(), $this->data)) {
                        $model = new Model(Change::class, Change::getSpaceName());
                        $this->data[$model->table] = [];
                        $this->models[$model->table] = $model;
                    }
                    $this->data[Change::getSpaceName()][] = [
                        'id' => count($this->data[Change::getSpaceName()]) + 1,
                        'listener' => $subscription['listener'],
                        'table' => $table,
                        'action' => $action,
                        'data' => $data,
                        'context' => $this->context,
                    ];
                }
            }
        }
    }

    public function ackChanges(string $listener, array $changes): void
    {
        foreach ($changes as $change) {
            $this->delete(Change::getSpaceName(), $change->id);
        }
    }

    public function getChanges(string $listener, int $limit = 100): array
    {
        $changes = [];
        if (array_key_exists(Change::getSpaceName(), $this->data)) {
            foreach ($this->data[Change::getSpaceName()] as $change) {
                if ($change['listener'] == $listener) {
                    $changes[] = new Change(...array_values($change));
                    if (count($changes) >= $limit) {
                        break;
                    }
                }
            }
        }
        return $changes;
    }
}
