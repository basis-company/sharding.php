<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Tier;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Domain as DomainInterface;
use Basis\Sharding\Interface\Segment as SegmentInterface;
use Basis\Sharding\Schema\Model;
use Closure;
use Tarantool\Mapper\Repository;
use Exception;
use ReflectionClass;

class Schema
{
    public array $casting = [];
    public array $classes = [];
    public array $models = [];

    public array $references = [];
    public array $collections = [];

    private ?Closure $resolver = null;

    public function __construct()
    {
        $this->register(Bucket::class);
        $this->register(Sequence::class);
        $this->register(Storage::class);
        $this->register(Tier::class);
        $this->register(Topology::class);
    }

    public function addReference(Reference $reference): void
    {
        $this->references[] = $reference;
    }

    public function getCollection(string $class, string $name): ?array
    {
        if (!count($this->collections)) {
            foreach ($this->references as $reference) {
                if (class_exists($reference->destination)) {
                    $this->collections[] = [
                        'class' => $reference->destination,
                        'destination' => $reference->model,
                        'name' => (new ReflectionClass($reference->model))->getShortName(),
                        'property' => $reference->property,
                    ];
                }
            }
        }

        foreach ($this->collections as $collection) {
            if ($collection['class'] == $class && $collection['name'] == $name) {
                return $collection;
            }
        }

        return null;
    }

    public function getModel(string|Model $table): Model
    {
        if ($table instanceof Model) {
            return $table;
        }

        if (!$table) {
            throw new Exception("Empty model casting");
        }

        if (array_key_exists($table, $this->casting)) {
            return $this->casting[$table];
        }

        if (class_exists($table)) {
            $classTable = $this->getTable($table);
            if ($classTable) {
                return $this->casting[$table] = $this->getModel($classTable);
            }

            return $this->casting[$table] = $this->register($table);
        }

        if (str_contains($table, '.')) {
            $table = str_replace('.', '_', $table);
        }

        if ($table && array_key_exists($table, $this->models)) {
            return $this->casting[$table] = $this->models[$table];
        }

        if ($this->resolver !== null) {
            $model = ($this->resolver)($table);
            if ($model && $model instanceof Model) {
                foreach ($this->models as $candidate) {
                    if ($candidate == $model) {
                        return $this->casting[$table] = $candidate;
                    }
                }
                return $this->casting[$table] = $this->registerModel($model);
            }
        }

        if (str_contains($table, '_')) {
            [$segment] = explode('_', $table);
            return $this->casting[$table] = $this->registerModel(new Model($segment, $table));
        }

        throw new Exception("Invalid model casting: $table");
    }

    public function getTable(string $class): ?string
    {
        return array_search($class, $this->classes) ?: null;
    }

    public function getReference(string $class, string $property): ?array
    {
        $property = self::toUnderscore($property);

        foreach ($this->references as $reference) {
            if ($reference->model == $class && self::toUnderscore($reference->property) == $property) {
                $destination = $reference->destination;
                if (!class_exists($destination) && !str_contains($destination, '.') && !$this->hasTable($destination)) {
                    // local entity domain
                    $destination = $this->getModel($reference->model)->segment . '.' . $destination;
                }

                return [
                    'class' => $destination,
                    'property' => $reference->property,
                ];
            }
        }

        return null;
    }

    public function getModels(?string $segment = null): array
    {
        if ($segment === null) {
            return $this->models;
        }

        return array_values(array_filter($this->models, fn ($model) => $model->segment == $segment));
    }

    public function getTableClass(string $table): ?string
    {
        return array_key_exists($table, $this->classes) ? $this->classes[$table] : '';
    }

    public function hasSegment(string $name): bool
    {
        return count($this->getModels($name)) > 0;
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, $this->classes);
    }

    public function isSharded(string $segment): bool
    {
        foreach ($this->models as $model) {
            if ($model->segment == $segment && $model->isSharded() || $model->hasTier()) {
                return true;
            }
        }

        return false;
    }

    public function register(string $class, ?string $domain = null): Model
    {
        if (array_key_exists($class, $this->classes)) {
            throw new Exception("Class $class already registered");
        }

        $parts = explode("\\", $class);
        $name = array_pop($parts); // name

        if (class_exists(Repository::class, false) && is_a($class, Repository::class, true)) {
            foreach ($this->models as $model) {
                if ($model->shortName == $name) {
                    $model->append($class);
                    return $model;
                }
            }
        }

        if ($domain === null) {
            if (is_a($class, DomainInterface::class, true)) {
                $domain = $class::getDomain();
            } else {
                $domain = array_pop($parts);
                if ($domain == 'Entity') {
                    $domain = count($parts) ? array_pop($parts) : 'Default';
                }
            }
            $domain = self::toUnderscore($domain);
        }


        $postfix = '';
        if (is_a($class, SegmentInterface::class, true)) {
            $postfix = $class::getSegment();
        }

        $key = self::toUnderscore($domain . ($postfix ? '_' . $postfix : ''));
        $table = $domain . '_' . Schema::toUnderscore(array_reverse(explode('\\', $class))[0]);

        return $this->registerModel(new Model($key, $table, $class));
    }

    public function registerModel(Model $model): Model
    {
        if (array_key_exists($model->table, $this->classes)) {
            if ($this->models[$model->table]->class === null && $model->class) {
                $this->classes[$model->table] = $model->class;
                $this->models[$model->table]->setClass($model->class);
                return $this->models[$model->table];
            }
            throw new Exception('Model ' . ($model->class ?: $model->table) . ' already registered');
        }

        $this->classes[$model->table] = $model->class;
        $this->models[$model->table] = $model;

        foreach ($model->getReferences() as $reference) {
            $this->collections = [];
            $this->addReference(clone $reference);
        }

        return $model;
    }

    public function setResolver(Closure $resolver): self
    {
        $this->resolver = $resolver;
        return $this;
    }

    public static function toCamelCase(string $string)
    {
        return implode('', array_map(ucfirst(...), explode('_', $string)));
    }

    public static function toUnderscore(string $string)
    {
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $string));
    }
}
