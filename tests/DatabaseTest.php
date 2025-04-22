<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Meta;
use Basis\Sharded\Database;
use Basis\Sharded\Schema\Model;
use Basis\Sharded\Test\Entity\MapperLogin;
use Basis\Sharded\Test\Entity\User;
use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    public function testMapperEntity()
    {
        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $driver->mapper->createSpace('mapper_login')->setClass(MapperLogin::class);
        $driver->mapper->migrate();
        $driver->mapper->findOrCreate(MapperLogin::class, ['username' => 'nekufa']);

        $this->assertCount(1, $driver->mapper->find(MapperLogin::class));

        $database = new Database(new Meta(), $driver);
        $database->meta->register(MapperLogin::class);
        $schema = $database->meta->getClassSegment(MapperLogin::class);
        $this->assertCount(1, $schema->getModels());
        [$model] = $schema->getModels();
        $this->assertInstanceOf(Model::class, $model);
        assert($model instanceof Model);
        $this->assertCount(2, $model->getIndexes());
        $this->assertSame(['id'], $model->getIndexes()[0]->fields);
        // index was defined using static initSchema method
        $this->assertSame(['username'], $model->getIndexes()[1]->fields);

        $this->assertCount(0, $database->locate(MapperLogin::class));
        $this->assertCount(1, $database->locate(MapperLogin::class, create: true));
        [$bucket] = $database->locate(MapperLogin::class, create: true);

        $database->update(Bucket::class, $bucket->id, [
            'flags' => Bucket::DEDICATED_FLAG,
            'storage' => 1,
        ]);

        $this->assertCount(1, $database->find(MapperLogin::class));
    }

    public function testRuntime()
    {
        Storage::$drivers = [];
        $database = new Database(new Meta(), new Runtime());
        $this->assertCount(1, $database->find(Storage::class));
        $database->meta->register(User::class);
        // create first
        $database->create(User::class, ['name' => 'nekufa']);
        $this->assertCount(1, $database->find(User::class));
        // create second
        $database->create(User::class, ['name' => 'nekufa2']);
        $this->assertCount(2, $database->find(User::class));
        // find present row
        $database->findOrCreate(User::class, ['name' => 'nekufa']);
        $this->assertCount(2, $database->find(User::class));
        // create not found
        $database->findOrCreate(User::class, ['name' => 'nekufa3']);
        $this->assertCount(3, $database->find(User::class));
    }

    public function testTarantool()
    {
        Storage::$drivers = [];
        $meta = new Meta();
        $meta->register(User::class);

        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $this->assertFalse($driver->mapper->hasSpace('test_user'));

        $database = new Database($meta, $driver);
        $this->assertCount(1, $database->find(Storage::class));
        $this->assertFalse($driver->mapper->hasSpace('test_user'));
        $database->create(User::class, ['name' => 'nekufa']);
        $this->assertTrue($driver->mapper->hasSpace('test_user'));
        $this->assertCount(1, $driver->mapper->find('test_user'));

        $database->create(User::class, ['name' => 'nekufa2']);
        $database->findOrCreate(User::class, ['name' => 'nekufa']);
        $this->assertCount(2, $driver->mapper->find('test_user'));

        $database->findOrCreate(User::class, ['name' => 'nekufa3']);
        $this->assertCount(3, $database->find(User::class));

        $database->update(User::class, 1, ['name' => 'nekufa14']);
        $this->assertCount(3, $database->find(User::class));
        $this->assertSame($database->findOrCreate(User::class, ['id' => 1], ['name' => '??'])->name, 'nekufa14');

        // test domain alias
        $this->assertCount(3, $database->find('test.user'));
        $this->assertInstanceOf(User::class, $database->find('test.user')[0]);

        $this->assertCount(3, $database->find('test_user'));
        $this->assertInstanceOf(User::class, $database->find('test_user')[0]);

        $meta = new Meta();
        $database = new Database($meta, $driver);
        // schema based names
        $this->assertCount(3, $database->find('test.user'));
        // prefixed table names
        $this->assertCount(3, $database->find('test_user'));

        $database->update('test.user', 2, ['name' => 'nekufa22']);
        $this->assertSame($database->findOrCreate('test.user', ['id' => 2], ['name' => '??'])->name, 'nekufa22');

        $database->update('test.user', 2, ['name' => 'nekufa23']);
        $this->assertSame($database->findOrFail('test.user', ['id' => 2])->name, 'nekufa23');
    }
}
