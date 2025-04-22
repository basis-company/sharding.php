<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Schema;
use Basis\Sharded\Database;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Schema\Model;
use Basis\Sharded\Test\Entity\Document;
use Basis\Sharded\Test\Entity\MapperLogin;
use Basis\Sharded\Test\Entity\User;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    public static function provideDrivers(): array
    {
        return [
            'tarantool' => [new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"))],
            'runtime' => [new Runtime()],
        ];
    }

    #[DataProvider('provideDrivers')]
    public function testStrings(Driver $driver)
    {
        $database = new Database(new Schema(), $driver->reset());
        $database->schema->register(Document::class);
        $this->assertCount(1, $database->find(Storage::class));
        $document = $database->create(Document::class, ['name' => 'test']);
        $this->assertNotNull($document->id);
        $document2 = $database->findOrCreate(Document::class, ['name' => 'test2']);
        $this->assertNotNull($document2->id);
        $this->assertCount(3, $database->find(Sequence::class));
    }

    #[DataProvider('provideDrivers')]
    public function testClassCasting(Driver $driver)
    {
        $database = new Database(new Schema(), $driver->reset());
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

        $database = new Database(new Schema(), $driver);
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

        $this->assertCount(0, $database->locate(MapperLogin::class));
        $this->assertCount(1, $database->locate(MapperLogin::class, create: true));
        [$bucket] = $database->locate(MapperLogin::class, create: true);

        $database->update(Bucket::class, $bucket->id, [
            'flags' => Bucket::DEDICATED_FLAG,
            'storage' => 1,
        ]);

        $this->assertCount(1, $database->find(MapperLogin::class));
    }

    #[DataProvider('provideDrivers')]
    public function testCrud(Driver $driver)
    {
        $database = new Database(new Schema(), $driver->reset());
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

    #[DataProvider('provideDrivers')]
    public function testTarantool(Driver $driver)
    {
        $schema = new Schema();
        $schema->register(User::class);

        $database = new Database($schema, $driver->reset());
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
        $database = new Database($schema, $driver);
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
