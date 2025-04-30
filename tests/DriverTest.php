<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Test\Entity\User;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class DriverTest extends TestCase
{
    public static function provideDrivers(): array
    {
        return [
            // 'tarantool' => [new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"))],
            'runtime' => [new Runtime()],
        ];
    }

    #[DataProvider('provideDrivers')]
    public function testChanges(Driver $driver)
    {
        $db = new Database($driver->reset());
        $db->schema->register(User::class);
        $nekufa = $db->create(User::class, ['name' => 'Dmitry Krokhin']);

        $this->assertCount(1, $driver->find('test_user'));
        $this->assertCount(0, $driver->getChanges('notifier'));
        $db->delete($nekufa);
        $this->assertCount(0, $driver->find('test_user'));

        $driver->registerChanges('test_user', 'notifier');
        $driver->setContext(['access' => 1]);

        $nekufa = $db->create(User::class, ['name' => 'Dmitry Krokhin']);
        $this->assertCount(1, $driver->find(Change::getSpaceName()));
        $this->assertCount(1, $driver->find('test_user'));
        $this->assertCount(1, $driver->getChanges('notifier'));

        [$change] = $driver->getChanges('notifier');
        $this->assertEquals($nekufa->id, $change->data['id']);
        $this->assertEquals('test_user', $change->table);
        $this->assertEquals('create', $change->action);
        $this->assertEquals(['access' => 1], $change->context);

        $driver->ackChanges('notifier', [$change]);
        $this->assertCount(0, $driver->getChanges('notifier'));
        $this->assertCount(0, $driver->find(Change::getSpaceName()));

        // wildcard test
        $driver->registerChanges('*', "event");
        // deduplication test
        $driver->registerChanges('*', "notifier");

        $nekufa = $db->create(User::class, ['name' => 'Dmitry Krokhin']);
        $this->assertCount(2, $driver->find(Change::getSpaceName()));
        $this->assertCount(2, $driver->getChanges());
        $this->assertCount(1, $driver->getChanges('event'));
        $this->assertCount(1, $driver->getChanges('notifier'));
    }
}
