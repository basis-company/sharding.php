<?php

namespace Basis\Sharding\Schema;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Attribute\Sharding as ShardingAttribute;
use Basis\Sharding\Attribute\Tier as TierAttribute;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Sharding as ShardingInterface;
use ReflectionClass;
use ReflectionProperty;
use Tarantool\Mapper\Space;
use Tarantool\Mapper\Repository;

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
    private string $tier = '';
    private ?Caching $cache = null;

    public function __construct(
        public readonly string $class,
        public readonly string $table,
    ) {
        $reflection = new ReflectionClass($class);
        if (count($reflection->getAttributes(Caching::class))) {
            $this->cache = $reflection->getAttributes(Caching::class)[0]->newInstance();
        }
        foreach ($reflection->getConstructor()?->getParameters() ?: [] as $parameter) {
            $this->properties[] = new Property($parameter->getName(), $parameter->getType()->getName());
        }
        if (!count($this->properties)) {
            foreach ($reflection->getProperties(ReflectionProperty::IS_PUBLIC) as $property) {
                if ($property->hasType()) {
                    $type = $property->getType()->getName();
                } else {
                    foreach (explode(PHP_EOL, $property->getDocComment()) as $line) {
                        if (str_contains($line, '@var')) {
                            $type = trim(explode('@var ', $line)[1]);
                            $type = match ($type) {
                                ucfirst($type) => 'int',
                                'integer' => 'int',
                                default => $type,
                            };
                        }
                    }
                }
                $this->properties[] = new Property($property->getName(), $type);
            }
        }

        if (is_a($class, ShardingInterface::class, true)) {
            $this->isSharded = true;
        } elseif (count($reflection->getAttributes(ShardingAttribute::class))) {
            $this->isSharded = true;
        }

        if (count($reflection->getAttributes(TierAttribute::class))) {
            $this->tier = $reflection->getAttributes(TierAttribute::class)[0]->newInstance()->name;
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

    public function append(string $class)
    {
        if (class_exists(Repository::class, false) && is_a($class, Repository::class, true)) {
            foreach ((new $class())->indexes as $index) {
                if (array_is_list($index)) {
                    $index = [
                        'fields' => $index,
                    ];
                }
                if (!array_key_exists('unique', $index)) {
                    $index['unique'] = false;
                }
                $this->indexes[] = new Index($index['fields'], $index['unique']);
            }
        }
    }

    /**
     * @return Index[]
     */
    public function getIndexes(): array
    {
        $indexes = [];
        foreach ($this->indexes as $index) {
            $indexes[$index->name] = $index;
        }
        return array_values($indexes);
    }

    /**
     * @return Property[]
     */
    public function getProperties(): array
    {
        return $this->properties;
    }

    public function getTable(?Bucket $bucket = null, ?Storage $storage = null): string
    {
        $table = $this->table;
        if ($bucket && $storage && $storage->isDedicated()) {
            $table = substr($table, strlen($bucket->name) + 1);
        }

        return $table;
    }

    public function getCache(): ?Caching
    {
        return $this->cache;
    }

    public function getTier(): string
    {
        return $this->tier;
    }

    public function isSharded(): bool
    {
        return $this->isSharded;
    }

    public function hasTier(): bool
    {
        return $this->tier != '';
    }
}
