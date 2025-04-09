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
        $router->create(User::class, ['name' => 'nekufa']);
        $router->create(User::class, ['name' => 'nekufa2']);
        $this->assertCount(2, $router->find(User::class));
    }

    public function testTarantool()
    {
        Storage::$drivers = [];
        $registry = new Registry();
        $registry->register(User::class);

        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();

        $router = new Router($registry, $driver);
        $this->assertCount(1, $router->find(Storage::class));
        $router->create(User::class, ['name' => 'nekufa']);
        $router->create(User::class, ['name' => 'nekufa2']);
        $router->findOrCreate(User::class, ['name' => 'nekufa']);

        // test domain alias
        $this->assertCount(2, $router->find('test.user'));
        $this->assertInstanceOf(User::class, $router->find('test.user')[0]);

        $registry = new Registry();
        $router = new Router($registry, $driver);
        // schema based names
        $this->assertCount(2, $router->find('test.user'));
        // prefixed table names
        $this->assertCount(2, $router->find('test_user'));
    }
}
