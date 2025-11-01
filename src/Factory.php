<?php

namespace Basis\Sharding;

use Basis\Sharding\Schema\Model;
use Closure;
use Exception;
use ReflectionClass;

class Factory
{
    private array $afterCreate = [];
    private array $defaults = [];
    private array $excludedFromIdentityMap = [];
    private array $identityMap = [];
    private bool $propertySorter = true;

    public function afterCreate(Closure $hook)
    {
        $this->afterCreate[] = $hook;
    }

    public function excludeFromIdentityMap(string $class)
    {
        $this->excludedFromIdentityMap[$class] = $class;

        if (array_key_exists($class, $this->identityMap)) {
            unset($this->identityMap[$class]);
        }
    }

    public function getDefaults(string $class): array
    {
        if (!array_key_exists($class, $this->defaults)) {
            $this->defaults[$class] = [];
            if (!class_exists($class)) {
                return [];
            }
            $reflection = new ReflectionClass($class);
            if (!$reflection->getConstructor()) {
                return [];
            }
            foreach ($reflection->getConstructor()->getParameters() as $parameter) {
                if ($parameter->isDefaultValueAvailable()) {
                    $this->defaults[$class][$parameter->getName()] = $parameter->getDefaultValue();
                }
            }
        }

        return $this->defaults[$class];
    }

    public function getInstance(Model $model, array|object $data): object
    {
        if (is_object($data)) {
            $data = get_object_vars($data);
        }

        if (array_key_exists('id', $data) && array_key_exists($model->table, $this->identityMap)) {
            if (array_key_exists($data['id'], $this->identityMap[$model->table])) {
                foreach ($data as $k => $v) {
                    $this->identityMap[$model->table][$data['id']]->$k = $v;
                }

                return $this->identityMap[$model->table][$data['id']];
            }
        }

        $instance = (object) $data;

        if ($model->class && class_exists($model->class)) {
            if (method_exists($model->class, '__construct')) {
                if ($this->propertySorter) {
                    $arguments = array_map(
                        fn($property) => $data[$property->name],
                        $model->getProperties(),
                    );
                } else {
                    $arguments = array_values($data);
                }
                $instance = new ($model->class)(...$arguments);
            } else {
                $instance = new ($model->class)();
                foreach ($data as $key => $value) {
                    $instance->$key = $value;
                }
            }
        }

        if (array_key_exists('id', $data) && !array_key_exists($model->class, $this->excludedFromIdentityMap)) {
            if (!array_key_exists($model->class, $this->identityMap)) {
                $this->identityMap[$model->class] = [];
            }
            $this->identityMap[$model->table][$instance->id] = $instance;
        }

        array_map(fn ($callback) => $callback($instance), $this->afterCreate);

        return $instance;
    }

    public function reset()
    {
        $this->identityMap = [];
    }

    public function setPropertySorter(bool $propertySorter): self
    {
        $this->propertySorter = $propertySorter;

        return $this;
    }
}
