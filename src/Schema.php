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
use Basis\Sharding\Schema\Segment;
use Tarantool\Mapper\Repository;
use Exception;
use ReflectionClass;

class Schema
{
    public array $classes = [];
    public array $segments = [];
    public array $references = [];
    public array $collections = [];

    public array $classSegment = [];
    public array $tableClass = [];
    public array $tableSegment = [];

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

    public function getClassModel(string $class): ?Model
    {
        return $this->getClassSegment($class)?->getClassModel($class);
    }

    public function getClassSegment(string $class): ?Segment
    {
        if (!array_key_exists($class, $this->classSegment)) {
            return null;
        }
        return $this->getSegmentByName($this->classSegment[$class]);
    }

    public function getClassTable(string $class): string
    {
        return $this->getSegmentByName($this->classSegment[$class])->getTable($class);
    }

    public function getReference(string $class, string $property): ?array
    {
        $property = self::toUnderscore($property);

        foreach ($this->references as $reference) {
            if ($reference->model == $class && $reference->property == $property) {
                $destination = $reference->destination;
                if (!class_exists($destination) && !str_contains($destination, '.')) {
                    // local entity domain
                    $destination = $this->getClassSegment($reference->model)->domain . '.' . $destination;
                }

                return [
                    'class' => $destination,
                    'property' => $property,
                ];
            }
        }

        return null;
    }

    public function getSegmentByName(string $name, bool $create = true): Segment
    {
        if (!$this->hasSegment($name)) {
            if (!$create) {
                throw new Exception("Segment $name not found");
            }
            $parts = explode('_', $name, 2);
            $this->segments[$name] = new Segment($parts[0], $parts[1] ?? '');
        }
        return $this->segments[$name];
    }

    public function getTableClass(string $table): string
    {
        return $this->tableClass[$table];
    }

    public function getTableSegment(string $table): Segment
    {
        return $this->getSegmentByName($this->tableSegment[$table]);
    }

    public function hasSegment(string $name): bool
    {
        return array_key_exists($name, $this->segments);
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, $this->tableClass);
    }

    public function register(string $class, ?string $domain = null)
    {
        if (array_key_exists($class, $this->classes)) {
            throw new Exception("Class $class already registered");
        }

        $parts = explode("\\", $class);
        $name = array_pop($parts); // name

        if (class_exists(Repository::class, false) && is_a($class, Repository::class, true)) {
            foreach ($this->segments as $segment) {
                foreach ($segment->getClasses() as $candidate) {
                    if ((new ReflectionClass($candidate))->getShortName() == $name) {
                        $segment->getClassModel($candidate)->append($class);
                        return;
                    }
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
        $segment = $this->getSegmentByName($key);

        $segment->register($class);
        $this->classSegment[$class] = $key;

        $table = $segment->getTable($class);
        $this->tableSegment[$table] = $key;
        $this->tableClass[$table] = $class;

        foreach ($segment->getClassModel($class)->getReferences() as $reference) {
            $this->collections = [];
            $this->addReference(clone $reference);
        }
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
