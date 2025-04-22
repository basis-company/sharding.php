<?php

declare(strict_types=1);

namespace Basis\Sharded\Schema;

use Basis\Sharded\Meta;
use Exception;

class Segment
{
    public readonly string $fullname;
    private array $models = [];

    public function __construct(
        public readonly string $domain,
        public readonly string $name,
        public array $classTable = [],
    ) {
        $this->fullname = $name ? $domain . '_' . $name : $domain;
    }

    public function getClasses(): array
    {
        return array_keys($this->classTable);
    }

    public function getClassModel(string $class): Model
    {
        if (!array_key_exists($class, $this->classTable)) {
            throw new Exception("Class $class not registered");
        }

        if (!array_key_exists($class, $this->models)) {
            $this->models[$class] = new Model($class, $this->getTable($class));
        }

        return $this->models[$class];
    }

    public function getModels(): array
    {
        return array_map($this->getClassModel(...), $this->getClasses());
    }

    public function getTable(string $class): string
    {
        if (!array_key_exists($class, $this->classTable)) {
            throw new Exception('Class not registered');
        }
        return $this->domain . '_' . $this->classTable[$class];
    }

    public function register(string $class): self
    {
        if (array_key_exists($class, $this->classTable)) {
            throw new Exception("Class $class already registered");
        }

        $this->classTable[$class] = Meta::toUnderscore(array_reverse(explode('\\', $class))[0]);

        return $this;
    }
}
