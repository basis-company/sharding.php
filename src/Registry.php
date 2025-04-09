<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Domain;
use Basis\Sharded\Interface\Subdomain;
use Basis\Sharded\Schema\Model;
use Basis\Sharded\Schema\Schema;
use Exception;
use ReflectionClass;

class Registry
{
    private array $classTable = [];
    private array $tableDomain = [];

    public function __construct()
    {
        $this->register(Bucket::class);
        $this->register(Sequence::class);
        $this->register(Storage::class);
    }

    public function getClass(string $table): ?string
    {
        $class = array_search($table, $this->classTable);
        if (!$class) {
            $class = array_search(str_replace('.', '_', $table), $this->classTable);
        }

        return $class ?: null;
    }

    public function getClassDomain(string $class): string
    {
        if (!array_key_exists($class, $this->classTable)) {
            throw new Exception("Class $class not registered");
        }
        return $this->getTableDomain($this->classTable[$class]);
    }

    public function getClasses(?string $domain = null): array
    {
        return array_map(
            fn ($table) => array_search($table, $this->classTable),
            array_keys($this->tableDomain, $domain)
        );
    }

    public function getDomain(string $classOrTable): string
    {
        if (class_exists($classOrTable)) {
            return $this->getClassDomain($classOrTable);
        }
        if (array_key_exists($classOrTable, $this->tableDomain)) {
            return $this->getTableDomain($classOrTable);
        }

        if (str_contains($classOrTable, '.')) {
            return explode('.', $classOrTable)[0];
        }

        if (str_contains($classOrTable, '_')) {
            return explode('_', $classOrTable)[0];
        }

        throw new Exception("Invalid class or table $classOrTable");
    }

    public function getDomains(): array
    {
        return array_values(array_unique($this->tableDomain));
    }

    public function getSchema(string $domain): Schema
    {
        return new Schema(
            $domain,
            array_map(fn($class) => new Model($class, $this->getTable($class)), $this->getClasses($domain))
        );
    }

    public function getTable(string $class): string
    {
        if (str_contains($class, ".")) {
            return str_replace('.', '_', $class);
        }

        if (str_contains($class, "_")) {
            return $class;
        }

        if (!array_key_exists($class, $this->classTable)) {
            throw new Exception("Class $class not registered");
        }

        return $this->classTable[$class];
    }

    public function getTableDomain(string $table): string
    {
        if (!array_key_exists($table, $this->tableDomain)) {
            throw new Exception("Table $table not registered");
        }

        return $this->tableDomain[$table];
    }

    public function register(string $class)
    {
        if (array_key_exists($class, $this->classTable)) {
            throw new Exception("Class $class already registered");
        }

        $parts = explode("\\", $class);
        $table = array_pop($parts);
        $subdomain = null;

        if (is_a($class, Domain::class, true)) {
            $domain = $class::getDomain();
        } else {
            $domain = array_pop($parts);
            if ($domain == 'Entity') {
                $domain = count($parts) ? array_pop($parts) : 'Default';
            }
        }
        if (is_a($class, Subdomain::class, true)) {
            $subdomain = $domain . '_'  . $class::getSubdomain();
        }

        $domain = $this->toUnderscore($domain);
        $table = $this->toUnderscore((new ReflectionClass($class))->getShortName());
        $table = $domain . '_' . $table;
        $this->classTable[$class] = $table;
        $this->tableDomain[$table] = $this->toUnderscore($subdomain ?: $domain);
    }

    public function toUnderscore(string $string)
    {
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $string));
    }
}
