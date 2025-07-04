<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Test\Entity\User;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class ChangesTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testChanges(Driver $driver)
    {
        $db = new Database($driver->reset());
        $db->schema->register(User::class);
        $db->setContext(fn() => ['access' => 1]);

        $nekufa = $db->create(User::class, ['name' => 'Dmitry Krokhin']);

        $this->assertCount(1, $driver->find('test_user'));
        $this->assertCount(0, $driver->getChanges('notifier'));
        $db->delete($nekufa);
        $this->assertCount(0, $driver->find('test_user'));

        $driver->registerChanges('test_user', 'notifier');

        $nekufa = $db->create('test_user', ['name' => 'Dmitry Krokhin']);
        $this->assertCount(1, $driver->find(Change::TABLE));
        $this->assertCount(1, $driver->find('test_user'));
        $this->assertCount(1, $driver->getChanges('notifier'));

        [$change] = $driver->getChanges('notifier');
        $this->assertNotSame(0, $change->id);
        $this->assertEquals($nekufa->id, $change->data['id']);
        $this->assertEquals('test_user', $change->tablename);
        $this->assertEquals('create', $change->action);
        $this->assertEquals(['access' => 1], $change->context);

        $driver->ackChanges([$change]);
        $this->assertCount(0, $driver->getChanges('notifier'));
        $this->assertCount(0, $driver->find(Change::TABLE));

        $db->delete($nekufa);
        $this->assertCount(1, $driver->find(Change::TABLE));
        [$change] = $driver->getChanges('notifier');
        $this->assertEquals('test_user', $change->tablename);
        $this->assertEquals('delete', $change->action);
        $this->assertEquals(['access' => 1], $change->context);
        $driver->ackChanges([$change]);

        // wildcard test
        $driver->registerChanges('*', "event");
        // deduplication test
        $driver->registerChanges('*', "notifier");

        $nekufa = $db->create('test_user', ['name' => 'Dmitry Krokhin']);
        $this->assertCount(2, $driver->find(Change::TABLE));
        $this->assertCount(2, $driver->getChanges());
        $this->assertCount(1, $driver->getChanges('event'));
        $this->assertCount(1, $driver->getChanges('notifier'));
        $this->assertEquals('create', $driver->getChanges()[0]->action);
        $driver->ackChanges($driver->getChanges());

        // no changes
        $nekufa2 = $driver->findOrCreate('test_user', ['id' => $nekufa->id], ['name' => 'Dmitry Krokhin']);
        $this->assertSame($nekufa->id, $nekufa2->id);
        $this->assertCount(0, $driver->find(Change::TABLE));
        $this->assertCount(0, $driver->getChanges());
        $driver->delete('test_user', $nekufa->id);
        $driver->ackChanges($driver->getChanges());

        // created
        $nekufa = $driver->findOrCreate('test_user', ['id' => 1], ['id' => 1, 'name' => 'Dmitry Krokhin']);
        $this->assertCount(2, $driver->find(Change::TABLE));
        $this->assertCount(2, $driver->getChanges());
        $this->assertCount(1, $driver->getChanges('event'));
        $this->assertCount(1, $driver->getChanges('notifier'));
        $this->assertEquals('create', $driver->getChanges()[0]->action);
        $driver->ackChanges($driver->getChanges());
        $this->assertCount(0, $driver->find(Change::TABLE));

        // update
        $driver->update('test_user', $nekufa->id, ['name' => 'tester']);
        $this->assertCount(2, $driver->find(Change::TABLE));
        $this->assertCount(2, $driver->getChanges());
        $this->assertCount(1, $driver->getChanges('event'));
        $this->assertCount(1, $driver->getChanges('notifier'));
        $this->assertEquals('update', $driver->getChanges()[0]->action);
    }
}
