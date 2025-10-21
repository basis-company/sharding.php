<?php

namespace Basis\Sharding\Schema;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Attribute\Sharding as ShardingAttribute;
use Basis\Sharding\Attribute\Tier as TierAttribute;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Sharding as ShardingInterface;
use Exception;
use ReflectionClass;
use ReflectionProperty;
use Tarantool\Mapper\Repository;
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

    /**
     * @var Reference[]
     */
    private array $references = [];

    private bool $isSharded = false;
    private string $tier = '';
    private ?Caching $cache = null;
    
    public readonly string $shortName;

    public function __construct(
        public readonly string $segment,
        public readonly string $table,
        public ?string $class = null,
    ) {
        if ($class && class_exists($class)) {
            return $this->setClass($class);
        }
    }

    public function addIndex(array|Index $fields, bool $unique = false): self
    {
        if ($fields instanceof Index) {
            $index = $fields;
        } else {
            $index = new Index($fields, $unique);
        }

        if (!array_key_exists($index->name, $this->indexes)) {
            $this->indexes[$index->name] = $index;
        }

        return $this;
    }

    public function addProperty(string|Property $name, ?string $type = 'int'): self
    {
        if ($name instanceof Property) {
            $this->properties[] = $name;
        } else {
            $this->properties[] = new Property($name, $type);
        }

        return $this;
    }

    public function addReference(Reference $reference): self
    {
        $this->references[] = $reference;

        return $this;
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
                    $index['unique'] = true;
                }

                $this->addIndex(new Index($index['fields'], $index['unique']));
            }

            if (!count($this->getIndexes()) || !array_values($this->getIndexes())[0]->unique) {
                throw new \Exception('No primary key is set for ' . $class);
            }
        }
    }

    public function getCache(): ?Caching
    {
        return $this->cache;
    }

    /**
     * @return Index[]
     */
    public function getIndexes(): array
    {
        return array_values($this->indexes);
    }

    /**
     * @return Property[]
     */
    public function getProperties(): array
    {
        return $this->properties;
    }

    public function getReferences(): array
    {
        return $this->references;
    }

    public function getTable(?Bucket $bucket = null, ?Storage $storage = null): string
    {
        $table = $this->table;
        if ($bucket && $storage && $storage->isDedicated()) {
            $table = substr($table, strlen($bucket->name) + 1);
        }

        return $table;
    }

    public function getTier(): string
    {
        return $this->tier;
    }

    public function hasTier(): bool
    {
        return $this->tier != '';
    }

    public function isSharded(): bool
    {
        return $this->isSharded;
    }

    public function setCache(Caching $cache): self
    {
        $this->cache = $cache;
        return $this;
    }

    public function setClass(string $class)
    {
        if (count($this->properties)) {
            throw new Exception("Model override");
        }

        if ($this->class === null) {
            $this->class = $class;
        }

        $reflection = new ReflectionClass($class);
        $this->shortName = $reflection->getShortName();

        if (count($reflection->getAttributes(Caching::class))) {
            $this->setCache($reflection->getAttributes(Caching::class)[0]->newInstance());
        }

        foreach ($reflection->getConstructor()?->getParameters() ?: [] as $parameter) {
            foreach ($parameter->getAttributes(Reference::class) as $reference) {
                $this->addReference($reference->newInstance()->setSource($class, $parameter->getName()));
            }
            $this->addProperty(new Property($parameter->getName(), $parameter->getType()->getName()));
        }

        if (!count($this->getProperties())) {
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
                $this->addProperty(new Property($property->getName(), $type));
            }
        }

        if (is_a($class, ShardingInterface::class, true)) {
            $this->setSharded(true);
        } elseif (count($reflection->getAttributes(ShardingAttribute::class))) {
            $this->setSharded(true);
        }

        if (count($reflection->getAttributes(TierAttribute::class))) {
            $this->setTier($reflection->getAttributes(TierAttribute::class)[0]->newInstance()->name);
        }

        if (property_exists($class, 'id')) {
            $this->addIndex(new UniqueIndex(['id']));
        }

        if (is_a($class, Indexing::class, true)) {
            array_map($this->addIndex(...), $class::getIndexes());
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
            array_map($this->addIndex(...), $fake->indexes);
        }
    }


    public function setSharded(bool $sharded): self
    {
        $this->isSharded = $sharded;
        return $this;
    }

    public function setTier(string $tier): self
    {
        $this->tier = $tier;
        return $this;
    }
}
