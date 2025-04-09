<?php

namespace Basis\Sharded\Schema;

use Basis\Sharded\Interface\Indexing;
use ReflectionClass;

class Model
{
    /**
     * @var Index[]
     */
    private array $indexes = [];

    /**
     * @var Property[]
     */
    private array $properties = [];

    public function __construct(
        public readonly string $class,
        public readonly string $table,
    ) {
        $reflection = new ReflectionClass($class);
        foreach ($reflection->getConstructor()->getParameters() as $parameter) {
            $this->properties[] = new Property($parameter->getName(), $parameter->getType()->getName());
        }

        $this->indexes[] = new UniqueIndex([$this->properties[0]->name]);
        if (is_a($class, Indexing::class, true)) {
            $this->indexes = array_merge($this->indexes, $class::getIndexes());
        }
    }

    /**
     * @return Index[]
     */
    public function getIndexes(): array
    {
        return $this->indexes;
    }

    /**
     * @return Property[]
     */
    public function getProperties(): array
    {
        return $this->properties;
    }
}
