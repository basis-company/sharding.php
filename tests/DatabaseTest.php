<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Schema;
use Basis\Sharding\Database;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Job\Convert;
use Basis\Sharding\Schema\Model;
use Basis\Sharding\Schema\Property;
use Basis\Sharding\Schema\UniqueIndex;
use Basis\Sharding\Test\Entity\Document;
use Basis\Sharding\Test\Entity\MapperLogin;
use Basis\Sharding\Test\Entity\User;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

class DatabaseTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testModels(Driver $driver)
    {
        $model = new Model('basis', 'basis_user');

        // property using params
        $model->addProperty('id', 'int');
        $model->addProperty('nick', 'string');
        // property using instance
        $model->addProperty(new Property('name', 'string'));

        // index using params
        $model->addIndex(['id'], true);
        // index instance
        $model->addIndex(new UniqueIndex(['nick']));

        $this->assertCount(3, $model->getProperties());
        $this->assertCount(2, $model->getIndexes());

        // ignore existing index
        $model->addIndex(new UniqueIndex(['nick']));
        $this->assertCount(2, $model->getIndexes());

        // validate property registration
        $this->assertSame(['id', 'nick', 'name'], $model->getFields());
        $types = array_map(fn($property) => $property->type, $model->getProperties());
        $this->assertSame(['int', 'string', 'string'], $types);

        $database = new Database($driver->reset());
        $database->schema->registerModel($model);
        $driver->syncSchema($database, $database->getBuckets('basis_user')[0]);

        if ($driver instanceof Tarantool) {
            $space = $driver->getMapper()->getSpace('basis_user');
            $property = new ReflectionProperty($space::class, 'indexes');
            $this->assertCount(count($property->getValue($space)), $model->getIndexes());
        }

        $this->assertSame($database->schema->getModel('basis_user'), $model);

        $user = $database->create('basis_user', ['nick' => 'nekufa']);
        $this->assertSame($user->nick, 'nekufa');
        $this->assertSame($user->id, 1);

        $sameUser = $database->findOrFail($model, ['nick' => 'nekufa']);
        $this->assertSame($user, $sameUser);
        $this->assertSame($user, $database->findOne('basis.user', []));

        $database->schema->registerModel(
            (new Model('basis', 'basis_channel'))
                ->addProperty('id')
                ->addIndex(['id'], true)
        );

        $database->getCoreDriver()->syncSchema($database, $database->getBuckets('basis_channel')[0]);
        $this->assertTrue($driver->hasTable('basis_channel'));

        $this->assertCount(0, $database->find('basis_channel'));
        $this->assertNotCount(0, $database->find('basis_user'));

        // add property to empty table
        $database->schema->getModel('basis_channel')->addProperty('title', 'string')->addIndex(['title'], false);
        $database->getCoreDriver()->syncSchema($database, $database->getBuckets('basis_channel')[0]);
        $channel = $database->create('basis_channel', ['title' => 'tester']);
        $this->assertSame(array_keys(get_object_vars($channel)), ['id', 'title']);
        $this->assertSame($channel, $database->findOne('basis_channel', []));

        $this->assertNotSame(
            $database->findOne('basis_user', []),
            $database->findOne('basis_channel', [])
        );

        // convert non-empty table
        $database = new Database($driver);
        $database->schema->registerModel(
            (new Model('basis', 'basis_channel'))
                ->addProperty('id')
                ->addProperty('slug', 'string')
                ->addProperty('title', 'string')
                ->addIndex(['id'], true)
                ->addIndex(['slug'])
                ->addIndex(['title'], false)
        );
        $database->dispatch(new Convert('basis_channel'));
        $this->assertObjectHasProperty('slug', $database->findOne('basis_channel', []));

        $driver->syncSchema($database, $database->getBuckets('basis_channel')[0]);
        $channel = $database->create('basis_channel', ['slug' => 'tester2']);
        $this->assertSame(1, $database->findOne('basis_channel', ['slug' => ''])->id);
        $this->assertSame(2, $database->findOne('basis_channel', ['slug' => 'tester2'])->id);

        // convert table with wrong properties order
        $database->schema->registerModel(
            (new Model('basis', 'basis_entity'))
                ->addProperty('idle', 'int')
                ->addProperty('id', 'int')
                ->addProperty('nick', 'string')
                ->addIndex(['id'], true)
                ->addIndex(['idle'], false)
                ->addIndex(['idle', 'nick'], true)
        );
        $driver->syncSchema($database, $database->getBuckets('basis_entity')[0]);
        $entity = $database->create('basis_entity', ['nick' => 'first']);
        $entity = $database->create('basis_entity', ['nick' => 'second']);
        $entity = $database->findOne('basis_entity', ['id' => 1]);

        $database = new Database($driver);
        $database->schema->registerModel(
            (new Model('basis', 'basis_entity'))
                ->addProperty('id', 'int')
                ->addProperty('idle', 'int')
                ->addProperty('nick', 'string')
                ->addIndex(['id'], true)
                ->addIndex(['idle'], false)
                ->addIndex(['idle', 'nick'], true)
        );
        $database->dispatch(new Convert('basis_entity'));

        if ($driver instanceof Tarantool) {
            $space = $driver->getMapper()->getSpace('basis_user');
            $this->assertCount(count($property->getValue($space)), $model->getIndexes());
        }

        $entityNew = $database->findOne('basis_entity', ['id' => 1]);
        $entityKeys = array_keys(get_object_vars($entity));
        $entityNewKeys = array_keys(get_object_vars($entityNew));

        $this->assertNotSame($entityKeys, $entityNewKeys);
        $this->assertSame($entity->idle, $entityNew->idle);
        $this->assertSame($entity->id, $entityNew->id);
        $this->assertSame([$entityKeys[1], $entityKeys[0], $entityKeys[2]], $entityNewKeys);
    }

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
        $login = $database->schema->getModel(MapperLogin::class);
        $this->assertCount(1, $database->schema->getModels($login->segment));
        [$model] = $database->schema->getModels($login->segment);
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
