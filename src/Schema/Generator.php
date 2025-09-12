<?php

namespace Basis\Sharding\Schema;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Trait\ActiveRecord;
use Basis\Sharding\Trait\References;
use Exception;
use ReflectionClass;

class Generator
{
    public string $domain = '';
    public string $segment = '';

    public function __construct(
        public string $namespace,
        public string $class = '',
    ) {
        if ($class == '') {
            if (str_contains($namespace, '\\')) {
                $parts = explode('\\', $namespace);
                $this->class = array_pop($parts);
                $this->namespace = implode('\\', $parts);
            } else {
                [$this->class, $this->namespace] = [$namespace, $class];
            }
        }
    }
        
    protected bool $activeRecord = false;
    protected array $properties = [];
    protected array $indexes = [];
    protected array $references = [];
    
    public string $tab = '    ';

    public function setDomain(string $domain): self
    {
        $this->domain = $domain;
        return $this;
    }

    public function setSegment(string $segment): self
    {
        $this->segment = $segment;
        return $this;
    }

    public function add(Index|Property|Reference ...$instances)
    {
        foreach ($instances as $instance) {
            match (get_class($instance)) {
                Index::class => $this->indexes[] = $instance,
                UniqueIndex::class => $this->indexes[] = $instance,
                Property::class => $this->properties[$instance->name] = $instance,
                Reference::class => (
                    array_key_exists($instance->property, $this->properties)
                    ? $this->references[$instance->property] = $instance
                    : throw new Exception("Invalid property " . $instance->property)
                ),
                default => throw new Exception("Invalid add: " . get_class($instance)),
            };
        }
    }

    public function getBody(): string
    {
        $interfaces = array_map(
            fn ($class) => (new ReflectionClass($class))->getShortName(),
            array_filter([
                $this->domain ? Domain::class : null,
                $this->segment ? Segment::class : null,
                count($this->indexes) ? Indexing::class : null,
            ])
        );

        $implements = (count($interfaces) ? ' implements ' . implode(', ', $interfaces) : '');

        $lines = [];
        $lines[]= "class $this->class" . $implements;
        $lines[]= "{";

        if ($this->activeRecord) {
            $lines[] = $this->tab . 'use ActiveRecord;';
            if (count($this->references)) {
                $lines[] = $this->tab . 'use References;';
            }
            $lines[] = '';
        }

        if (count($this->properties)) {
            $lines[] = $this->tab. 'public function __construct(';
            foreach ($this->properties as $property) {
                if (array_key_exists($property->name, $this->references)) {
                    $reference = $this->references[$property->name];                    
                    $destination = '$reference->destination';
                    if (class_exists($reference->destination, false)) {
                        $reflection = new ReflectionClass($reference->destination);
                        $destination = $reflection->getShortName() . '::class';
                    } else {
                        $destination = "'$reference->destination'";
                    }
                    $lines[] = $this->tab . $this->tab . "#[Reference($destination)]";
                }
                $lines[] = $this->tab . $this->tab . "public $property->type \$$property->name,";
            }
            $lines[] = $this->tab. ') {';
            $lines[] = $this->tab. '}';
        }

        if ($this->domain) {
            $lines[] = implode(PHP_EOL . $this->tab, [
                '',

                'public static function getDomain(): string',
                '{',
                $this->tab . 'return "' . $this->domain . '";',
                '}',
            ]);
        }

        if (count($this->indexes)) {
            $lines[] = implode(PHP_EOL . $this->tab, [
                '',
                '/**',
                ' * @return Index[]',
                ' */',
                'public static function getIndexes(): array',
                '{',
                $this->tab . 'return [',
                implode(PHP_EOL, array_map(
                    fn ($row) => $this->tab . $this->tab . $row,
                    array_map(function ($index) {
                        $fields = '["' . implode('", "', $index->fields) . '"]';
                        $unique = var_export($index->unique, true);
                        return "new Index($fields, $unique),";
                    }, $this->indexes)
                )),
                $this->tab . '];',
                '}',
            ]);

        }

        if ($this->segment) {
            $lines[] = implode(PHP_EOL . $this->tab, [
                '',

                'public static function getSegment(): string',
                '{',
                $this->tab . 'return "' . $this->segment . '";',
                '}',
            ]);
        }

        $lines[]= "}";
        
        return implode(PHP_EOL, $lines);
    }
    
    public function getHeader(): string
    {
        $lines = [];

        if ($this->namespace) {
            $lines[] = "namespace $this->namespace;";
            $lines[] = '';
        }

        $usage = array_filter([
            $this->activeRecord ? ActiveRecord::class : null,
            $this->activeRecord && count($this->references) ? References::class : null,
            $this->domain ? Domain::class : null,
            $this->segment ? Segment::class : null,
            count($this->indexes) ? Index::class : null,
            count($this->indexes) ? Indexing::class : null,
            count($this->references) ? Reference::class : null,
        ]);

        foreach ($this->references as $reference) {
            if (class_exists($reference->destination, false)) {
                $usage[] = $reference->destination;
            }
        }

        if (count($usage)) {
            sort($usage);
            $lines = array_merge($lines, array_map(fn($cls) => "use $cls;", $usage), ['']);
        }

        return implode(PHP_EOL, $lines);
    }

    public function useActiveRecord(bool $activeRecord = false): self
    {
        $this->activeRecord = $activeRecord;
        return $this;
    }

    public function write(string $path, bool $ifNotExist = true): bool
    {
        if (!$ifNotExist || !file_exists($path)) {
            file_put_contents($path, '<?php ' . PHP_EOL . PHP_EOL . $this);
            return true;
        }

        return false;
    }


    public function __toString()
    {
        return $this->getHeader() . PHP_EOL . $this->getBody();
    }
}
