<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Registry;
use Basis\Sharded\Router;
use Basis\Sharded\Test\Entity\User;
use PHPUnit\Framework\TestCase;

class RouterTest extends TestCase
{
    public function testRuntime()
    {
        Storage::$drivers = [];
        $router = new Router(new Registry(), new Runtime());
        $this->assertCount(1, $router->find(Storage::class));
        $router->registry->register(User::class);
        // create first
        $router->create(User::class, ['name' => 'nekufa']);
        $this->assertCount(1, $router->find(User::class));
        // create second
        $router->create(User::class, ['name' => 'nekufa2']);
        $this->assertCount(2, $router->find(User::class));
        // find present row
        $router->findOrCreate(User::class, ['name' => 'nekufa']);
        $this->assertCount(2, $router->find(User::class));
        // create not found
        $router->findOrCreate(User::class, ['name' => 'nekufa3']);
        $this->assertCount(3, $router->find(User::class));
    }

    public function testTarantool()
    {
        Storage::$drivers = [];
        $registry = new Registry();
        $registry->register(User::class);

        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $this->assertFalse($driver->mapper->hasSpace('test_user'));

        $router = new Router($registry, $driver);
        $this->assertCount(1, $router->find(Storage::class));
        $this->assertFalse($driver->mapper->hasSpace('test_user'));
        $router->create(User::class, ['name' => 'nekufa']);
        $this->assertTrue($driver->mapper->hasSpace('test_user'));
        $this->assertCount(1, $driver->mapper->find('test_user'));

        $router->create(User::class, ['name' => 'nekufa2']);
        $router->findOrCreate(User::class, ['name' => 'nekufa']);
        $this->assertCount(2, $driver->mapper->find('test_user'));

        $router->findOrCreate(User::class, ['name' => 'nekufa3']);
        $this->assertCount(3, $router->find(User::class));

        // test domain alias
        $this->assertCount(3, $router->find('test.user'));
        $this->assertInstanceOf(User::class, $router->find('test.user')[0]);

        $registry = new Registry();
        $router = new Router($registry, $driver);
        // schema based names
        $this->assertCount(3, $router->find('test.user'));
        // prefixed table names
        $this->assertCount(3, $router->find('test_user'));
    }
}
