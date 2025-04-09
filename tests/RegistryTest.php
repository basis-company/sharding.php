<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Registry;
use Basis\Sharded\Test\Entity\User;
use Basis\Sharded\Test\Entity\Post;
use Exception;
use PHPUnit\Framework\TestCase;

class RegistryTest extends TestCase
{
    public function testSystemBucket()
    {
        $registry = new Registry();
        $this->assertSame($registry->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
        ]);
        $this->assertSame($registry->getTableDomain('sharded_bucket'), 'sharded_buckets');
        $this->assertSame($registry->getTableDomain('sharded_sequence'), 'sharded_sequences');
        $this->assertSame($registry->getTableDomain('sharded_storage'), 'sharded_storages');
    }

    public function testDuplicateClass()
    {
        $registry = new Registry();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Class ' . User::class . ' already registered');
        $registry->register(User::class);
        $registry->register(User::class);
    }

    public function testDomainCasting()
    {
        $registry = new Registry();
        $registry->register(User::class);
        $this->assertSame($registry->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test'
        ]);
        $this->assertSame($registry->getClasses('test'), [User::class]);
        $this->assertSame($registry->getTable(User::class), 'test_user');
    }

    public function testSubdomains()
    {
        $registry = new Registry();
        $registry->register(Post::class);
        $this->assertSame($registry->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test_posts'
        ]);
        $this->assertSame($registry->getClasses('test_posts'), [Post::class]);
        $this->assertSame($registry->getTable(Post::class), 'test_post');
    }
}
