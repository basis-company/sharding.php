<?php

namespace Basis\Sharding;

use Closure;
use ReflectionClass;

class Factory
{
    private array $afterCreate = [];
    private array $defaults = [];
    private array $excludedFromIdentityMap = [];
    private array $identityMap = [];

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

    public function getInstance(string $class, array|object $data): object
    {
        if (is_object($data)) {
            $data = get_object_vars($data);
        }

        if (array_key_exists('id', $data) && array_key_exists($class, $this->identityMap)) {
            if (array_key_exists($data['id'], $this->identityMap[$class])) {
                foreach ($data as $k => $v) {
                    $this->identityMap[$class][$data['id']]->$k = $v;
                }

                return $this->identityMap[$class][$data['id']];
            }
        }

        $instance = (object) $data;

        if ($class && class_exists($class)) {
            if (method_exists($class, '__construct')) {
                $instance = new $class(...array_values($data));
            } else {
                $instance = new $class();
                foreach ($data as $key => $value) {
                    $instance->$key = $value;
                }
            }
        }

        if (array_key_exists('id', $data) && !array_key_exists($class, $this->excludedFromIdentityMap)) {
            if (!array_key_exists($class, $this->identityMap)) {
                $this->identityMap[$class] = [];
            }
            $this->identityMap[$class][$instance->id] = $instance;
        }

        array_map(fn ($callback) => $callback($instance), $this->afterCreate);

        return $instance;
    }
}
