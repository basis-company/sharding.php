<?php

declare(strict_types=1);

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Schema\Legacy;
use Basis\Sharding\Test\Entity\StageCorrection;
use Basis\Sharding\Test\Repository\StageCorrection as StageCorrectionRepository;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class LegacyTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function test(Driver $driver)
    {
        Legacy::initialize();

        $database = new Database($driver->reset());
        $database->schema->register(StageCorrection::class, 'stage');
        $database->schema->register(StageCorrectionRepository::class);
        $this->assertCount(2, $database->schema->getClassModel(StageCorrection::class)->getIndexes());

        $database->create(Bucket::class, [
            'name' => 'stage',
            'flags' => Bucket::DEDICATED_FLAG
        ]);

        $correction = $database->create(StageCorrection::class, [
            'stage' => 123,
        ]);

        $this->assertTrue($driver->hasTable('stage_correction'));

        $this->assertInstanceOf(StageCorrection::class, $correction);
        $this->assertEquals(123, $correction->stage);
        $this->assertNotSame(0, $correction->id);

        $this->assertCount(1, $database->find(StageCorrection::class));
        $this->assertCount(1, $database->find('stage.stage_correction'));
        $this->assertInstanceOf(StageCorrection::class, $database->findOne('stage.stage_correction', []));

        $database = new Database($database->getCoreDriver());
        $this->assertCount(1, $database->find('stage.stage_correction'));
    }
}
