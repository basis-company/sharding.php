<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\Generator;
use Basis\Sharding\Schema\Property;
use Basis\Sharding\Schema\UniqueIndex;
use PHPUnit\Framework\TestCase;

class Generatortest extends TestCase
{
    public function testNamespaceAndClassCasting()
    {
        $generator = new Generator('Basis\\Web', 'Bundle');
        $this->assertSame($generator->namespace, 'Basis\\Web');
        $this->assertSame($generator->class, 'Bundle');

        $generator = new Generator('Tester');
        $this->assertSame($generator->namespace, '');
        $this->assertSame($generator->class, 'Tester');

        $generator = new Generator("Basis\\Guard\\Entity\\Access");
        $this->assertSame($generator->namespace, 'Basis\\Guard\\Entity');
        $this->assertSame($generator->class, 'Access');
    }

    public function test()
    {
        $generator = new Generator("Basis\\Guard\\Entity\\Access");
        $this->assertStringContainsString("namespace Basis\\Guard\\Entity;", $generator);
        $this->assertStringContainsString("class Access" . PHP_EOL, $generator);
        $this->assertStringNotContainsString("public function __construct", $generator);

        $generator->add(
            new Property("id", "int"),
            new Property("name", "string"),
            new UniqueIndex(['name']),
            new Property('user', 'int'),
            new Reference('guard.user', 'user'),
            new Property('test', 'string'),
            new Reference(self::class, 'test'),
        );

        $generator->setDomain('guard')->setSegment('session');

        $this->assertStringContainsString("public function __construct", $generator);
        $this->assertStringContainsString("use " . self::class.';' . PHP_EOL, $generator);
        $this->assertStringContainsString("use " . Indexing::class.';' . PHP_EOL, $generator);
        $this->assertStringNotContainsString("use ActiveRecord", $generator);
        $generator->useActiveRecord(true);
        $this->assertStringContainsString("use ActiveRecord", $generator);

        $this->assertFalse(class_exists(\Basis\Guard\Entity\Access::class, false));
        eval($generator);
        $this->assertTrue(class_exists(\Basis\Guard\Entity\Access::class, false));

        $access = new \Basis\Guard\Entity\Access(5, 'tester', 27, '4');
        $this->assertSame($access->id, 5);
        $this->assertSame($access->name, 'tester');
        $this->assertSame($access->user, 27);
        $this->assertSame($access->test, '4');
    }
}
