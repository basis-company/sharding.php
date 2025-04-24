<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Domain as DomainInterface;
use Basis\Sharded\Interface\Segment as SegmentInterface;
use Basis\Sharded\Schema\Model;
use Basis\Sharded\Schema\Segment;
use Exception;

class Schema
{
    public array $classes = [];
    public array $segments = [];

    public array $classSegment = [];
    public array $tableClass = [];
    public array $tableSegment = [];

    public function __construct()
    {
        $this->register(Bucket::class);
        $this->register(Sequence::class);
        $this->register(Storage::class);
    }

    public function getClassModel(string $class): Model
    {
        return $this->getClassSegment($class)->getClassModel($class);
    }

    public function getClassSegment(string $class): Segment
    {
        return $this->getSegmentByName($this->classSegment[$class]);
    }

    public function getSegmentByName(string $name): Segment
    {
        if (!array_key_exists($name, $this->segments)) {
            $parts = explode('_', $name, 2);
            $this->segments[$name] = new Segment($parts[0], $parts[1] ?? '');
        }
        return $this->segments[$name];
    }

    public function getClassTable(string $class): string
    {
        return $this->getSegmentByName($this->classSegment[$class])->getTable($class);
    }

    public function getTableClass(string $table): string
    {
        return $this->tableClass[$table];
    }

    public function getTableSegment(string $table): Segment
    {
        return $this->getSegmentByName($this->tableSegment[$table]);
    }

    public function hasTable(string $table): bool
    {
        return array_key_exists($table, $this->tableClass);
    }

    public function register(string $class)
    {
        if (array_key_exists($class, $this->classes)) {
            throw new Exception("Class $class already registered");
        }

        $parts = explode("\\", $class);
        array_pop($parts); // name

        if (is_a($class, DomainInterface::class, true)) {
            $domain = $class::getDomain();
        } else {
            $domain = array_pop($parts);
            if ($domain == 'Entity') {
                $domain = count($parts) ? array_pop($parts) : 'Default';
            }
        }
        $domain = self::toUnderscore($domain);

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
    }

    public static function toUnderscore(string $string)
    {
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $string));
    }
}
