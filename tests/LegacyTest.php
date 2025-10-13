<?php

declare(strict_types=1);

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Locator;
use Basis\Sharding\Schema\Legacy;
use Basis\Sharding\Test\Entity\StageCorrection;
use Basis\Sharding\Test\Repository\StageCorrection as StageCorrectionRepository;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class LegacyTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testDedicated(Driver $driver)
    {
        Legacy::initialize();

        $database = new Database($driver->reset());
        $database->schema->register(StageCorrection::class, 'stage');
        $database->schema->register(StageCorrectionRepository::class);
        $this->assertCount(2, $database->schema->getModel(StageCorrection::class)->getIndexes());

        $additionalStorage = $database->create(Storage::class, [
            'type' => array_search(get_class($driver), Storage::$types),
            'dsn' => $driver->getDsn(),
            'flags' => Storage::DEDICATED_FLAG,
        ]);

        $bucket = $database->create(Bucket::class, ['name' => 'stage']);

        $database->locator->assignStorage($bucket, Locator::class);
        $this->assertNotSame($bucket->storage, $additionalStorage->id);
        $database->update($bucket, ['storage' => $additionalStorage->id]);

        $correction = $database->create(StageCorrection::class, [
            'stage' => 123,
        ]);

        $this->assertTrue($additionalStorage->getDriver()->hasTable('stage_correction'));

        $this->assertInstanceOf(StageCorrection::class, $correction);
        $this->assertEquals(123, $correction->stage);
        $this->assertNotSame(0, $correction->id);

        $this->assertCount(1, $database->find(StageCorrection::class));
        $this->assertCount(1, $database->find('stage.stage_correction'));
        $this->assertInstanceOf(StageCorrection::class, $database->findOne('stage.stage_correction', []));

        // sequence init test
        $this->assertCount(1, $database->find(Sequence::class, ['name' => 'stage_stage_correction']));
        $database->delete($database->findOne(Sequence::class, ['name' => 'stage_stage_correction']));
        $this->assertCount(0, $database->find(Sequence::class, ['name' => 'stage_stage_correction']));

        $correction2 = $database->create(StageCorrection::class, ['stage' => 22]);
        $this->assertSame($correction2->id, 2);

        $database = new Database($database->getCoreDriver());

        if ($driver instanceof Runtime) {
            // copy data
            $database->getStorage(2)->getDriver()->data = $additionalStorage->getDriver()->data;
        }

        $this->assertCount(2, $database->find('stage.stage_correction'));

        // later class registration
        $model = $database->schema->register(StageCorrection::class, 'stage');
        $this->assertSame($database->schema->getModel('stage_stage_correction')->class, StageCorrection::class);
        $database->schema->getTableClass(str_replace('.', '_', 'stage.stage_correction'));

        $this->assertInstanceOf(StageCorrection::class, $database->findOne('stage.stage_correction', []));

        // domain test
        $this->assertCount(2, $database->getDomain('stage')->find('stage_correction'));
        $this->assertInstanceOf(StageCorrection::class, $database->getDomain('stage')->findOne('stage_correction', []));
    }

    public function testRuntimeFreeDataSelect()
    {
        $database = new Database(new Runtime());

        $this->assertCount(0, $database->find('guard.access'));
        $database->create('guard.access', ['hash' => 1]);

        $this->assertCount(1, $database->find('guard.access'));
        $this->assertObjectHasProperty('id', $database->findOne('guard.access', []));

        $database->findOrCreate('guard.access', ['hash' => 2]);
        $this->assertCount(2, $database->find('guard.access'));

        $database->findOrCreate('guard_access', ['hash' => 2]);
        $this->assertCount(2, $database->find('guard.access'));
    }

    public function testRuntimeFreeDataCreate()
    {
        $database = new Database(new Runtime());

        $database->create('guard.access', ['hash' => 1]);
        $this->assertCount(1, $database->find('guard.access'));
        $this->assertCount(1, $database->find('guard_access'));
    }
}
