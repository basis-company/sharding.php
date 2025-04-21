<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Meta;
use Basis\Sharded\Router;
use Basis\Sharded\Schema\Model;
use Basis\Sharded\Test\Entity\MapperLogin;
use Basis\Sharded\Test\Entity\User;
use PHPUnit\Framework\TestCase;

class RouterTest extends TestCase
{
    public function testMapperEntity()
    {
        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $driver->mapper->createSpace('mapper_login')->setClass(MapperLogin::class);
        $driver->mapper->migrate();
        $driver->mapper->findOrCreate(MapperLogin::class, ['username' => 'nekufa']);

        $this->assertCount(1, $driver->mapper->find(MapperLogin::class));

        $router = new Router(new Meta(), $driver);
        $router->meta->register(MapperLogin::class);
        $schema = $router->meta->getClassSegment(MapperLogin::class);
        $this->assertCount(1, $schema->getModels());
        [$model] = $schema->getModels();
        $this->assertInstanceOf(Model::class, $model);
        assert($model instanceof Model);
        $this->assertCount(2, $model->getIndexes());
        $this->assertSame(['id'], $model->getIndexes()[0]->fields);
        // index was defined using static initSchema method
        $this->assertSame(['username'], $model->getIndexes()[1]->fields);

        $this->assertCount(0, $router->getBuckets(MapperLogin::class));
        $this->assertCount(1, $router->getBuckets(MapperLogin::class, createIfNotExists: true));
        [$bucket] = $router->getBuckets(MapperLogin::class, createIfNotExists: true);

        $router->update(Bucket::class, $bucket->id, [
            'flags' => Bucket::DROP_PREFIX_FLAG,
            'storage' => 1,
        ]);

        $this->assertCount(1, $router->find(MapperLogin::class));
    }

    public function testRuntime()
    {
        Storage::$drivers = [];
        $router = new Router(new Meta(), new Runtime());
        $this->assertCount(1, $router->find(Storage::class));
        $router->meta->register(User::class);
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
        $meta = new Meta();
        $meta->register(User::class);

        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $this->assertFalse($driver->mapper->hasSpace('test_user'));

        $router = new Router($meta, $driver);
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

        $this->assertCount(3, $router->find('test_user'));
        $this->assertInstanceOf(User::class, $router->find('test_user')[0]);

        $meta = new Meta();
        $router = new Router($meta, $driver);
        // schema based names
        $this->assertCount(3, $router->find('test.user'));
        // prefixed table names
        $this->assertCount(3, $router->find('test_user'));
    }
}
