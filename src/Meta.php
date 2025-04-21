<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Domain as DomainInterface;
use Basis\Sharded\Interface\Segment as SegmentInterface;
use Basis\Sharded\Schema\Segment;
use Exception;

class Meta
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

    public function getClassSegment(string $class): Segment
    {
        return $this->getSegmentByName($this->classSegment[$class]);
    }

    public function getSegmentByName(string $name): Segment
    {
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

    public function getTableSegment(string $table): string
    {
        return $this->tableSegment[$table];
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

        $segment = '';
        if (is_a($class, SegmentInterface::class, true)) {
            $segment = $class::getSegment();
        }

        $key = self::toUnderscore($domain . ($segment ? '_' . $segment : ''));
        if (!array_key_exists($key, $this->segments)) {
            $this->segments[$key] = new Segment($domain, $segment);
        }

        $this->segments[$key]->register($class);
        $this->classSegment[$class] = $key;

        $table = $this->segments[$key]->getTable($class);
        $this->tableSegment[$table] = $key;
        $this->tableClass[$table] = $class;
    }

    public static function toUnderscore(string $string)
    {
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $string));
    }
}
