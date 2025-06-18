<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Schema;
use Basis\Sharding\Database;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Schema\Model;
use Basis\Sharding\Test\Entity\Document;
use Basis\Sharding\Test\Entity\MapperLogin;
use Basis\Sharding\Test\Entity\User;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testStrings(Driver $driver)
    {
        $database = new Database($driver->reset());
        $database->schema->register(Document::class);
        $this->assertCount(1, $database->find(Storage::class));

        $document = $database->create(Document::class, ['name' => 'test']);
        $this->assertNotNull($document->id);

        $document2 = $database->findOrCreate(Document::class, ['name' => 'test2']);
        $this->assertNotNull($document2->id);
        $document2x = $database->findOrCreate(Document::class, ['name' => 'test2']);
        $this->assertSame($document2->id, $document2x->id);
        $this->assertCount(3, $database->find(Sequence::class));
    }

    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testClassCasting(Driver $driver)
    {
        $database = new Database($driver->reset());
        $this->assertCount(1, $database->find(Storage::class));
        $database->schema->register(User::class);

        $nekufa = $database->create(User::class, ['name' => 'Dmitry Krokhin']);
        $this->assertCount(1, $database->find(User::class));
        $nekufa = $database->update($nekufa, ['name' => 'Nekufa']);

        $this->assertSame('Nekufa', $nekufa->name);
        $this->assertSame('Nekufa', $database->find(User::class)[0]->name);

        $user = $database->delete($nekufa);
        $this->assertNotNull($user);
        $this->assertSame($user->name, 'Nekufa');
    }

    public function testMapperEntity()
    {
        $driver = new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"));
        $driver->mapper->dropUserSpaces();
        $driver->mapper->createSpace('mapper_login')->setClass(MapperLogin::class);
        $driver->mapper->migrate();
        $driver->mapper->findOrCreate(MapperLogin::class, ['username' => 'nekufa']);

        $this->assertCount(1, $driver->mapper->find(MapperLogin::class));

        $database = new Database($driver);
        $database->schema->register(MapperLogin::class);
        $schema = $database->schema->getClassSegment(MapperLogin::class);
        $this->assertCount(1, $schema->getModels());
        [$model] = $schema->getModels();
        $this->assertInstanceOf(Model::class, $model);
        assert($model instanceof Model);
        $this->assertCount(2, $model->getIndexes());
        $this->assertSame(['id'], $model->getIndexes()[0]->fields);
        // index was defined using static initSchema method
        $this->assertSame(['username'], $model->getIndexes()[1]->fields);

        $this->assertCount(1, $database->getBuckets(MapperLogin::class, writable: true));
        [$bucket] = $database->getBuckets(MapperLogin::class, writable: true);

        $database->update($bucket, ['storage' => 1]);
        $database->update($database->getStorage(1), [ 'flags' => Storage::DEDICATED_FLAG ]);

        $this->assertCount(1, $database->find(MapperLogin::class));
    }

    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testCrud(Driver $driver)
    {
        $database = new Database($driver->reset());
        $this->assertCount(1, $database->find(Storage::class));
        $database->schema->register(User::class);
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
        $nekufa3 = $database->findOrCreate(User::class, ['name' => 'nekufa3']);
        $this->assertSame($nekufa3->id, 3);
        $this->assertCount(3, $database->find(User::class));

        $user = $database->delete(User::class, 3);
        $this->assertNotNull($user);
        $this->assertSame($user->name, 'nekufa3');
        $this->assertCount(2, $database->find(User::class));
    }

    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testTarantool(Driver $driver)
    {
        $schema = new Schema();
        $schema->register(User::class);

        $database = new Database($driver->reset(), $schema);
        $this->assertCount(1, $database->find(Storage::class));
        $database->create(User::class, ['name' => 'nekufa']);

        $database->create(User::class, ['name' => 'nekufa2']);
        $database->findOrCreate(User::class, ['name' => 'nekufa']);

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

        $schema = new Schema();
        $database = new Database($driver, $schema);
        // schema based names
        $this->assertCount(3, $database->find('test.user'));
        // prefixed table names
        $this->assertCount(3, $database->find('test_user'));

        $database->update('test.user', 2, ['name' => 'nekufa22']);
        $this->assertSame($database->findOrCreate('test.user', ['id' => 2], ['name' => '??'])->name, 'nekufa22');

        $database->update('test.user', 2, ['name' => 'nekufa23']);
        $this->assertSame($database->findOrFail('test.user', ['id' => 2])->name, 'nekufa23');

        $user = $database->delete('test.user', 3);
        $this->assertNotNull($user);
        $this->assertSame($user->name, 'nekufa3');
        $this->assertCount(2, $database->find('test.user'));
    }
}
