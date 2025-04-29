<?php

namespace Basis\Sharding\Schema;

use Basis\Sharding\Attribute\Sharding as ShardingAttribute;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Sharding as ShardingInterface;
use ReflectionClass;
use Tarantool\Mapper\Space;

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

    private bool $isSharded = false;

    public function __construct(
        public readonly string $class,
        public readonly string $table,
    ) {
        $reflection = new ReflectionClass($class);
        foreach ($reflection->getConstructor()->getParameters() as $parameter) {
            $this->properties[] = new Property($parameter->getName(), $parameter->getType()->getName());
        }

        if (is_a($class, ShardingInterface::class, true)) {
            $this->isSharded = true;
        } elseif (count($reflection->getAttributes(ShardingAttribute::class))) {
            $this->isSharded = true;
        }

        $this->indexes[] = new UniqueIndex([$this->properties[0]->name]);
        if (is_a($class, Indexing::class, true)) {
            $this->indexes = array_merge($this->indexes, $class::getIndexes());
        } elseif (method_exists($class, 'initSchema')) {
            $class::initSchema($fake = new class extends Space {
                public function __construct(public array $indexes = [])
                {
                }
                public function addIndex(array $fields, array $options = []): void
                {
                    $this->indexes[] = new Index($fields, $options['unique'] ?? false);
                }
            });
            $this->indexes = array_merge($this->indexes, $fake->indexes);
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

    public function isSharded(): bool
    {
        return $this->isSharded;
    }
}
