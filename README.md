# A Sharding Library for PHP
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/basis-company/sharding.php)
[![Latest Version](https://img.shields.io/github/release/basis-company/sharding.php.svg)](https://github.com/basis-company/sharding.php/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Testing](https://github.com/basis-company/sharding.php/actions/workflows/tests.yml/badge.svg)](https://github.com/basis-company/sharding.php/actions/workflows/tests.yml)
[![Total Downloads](https://img.shields.io/packagist/dt/basis-company/sharding.svg)](https://packagist.org/packages/basis-company/sharding)

## Overview

Sharding is a php library designed to handle domain data that is split into segments, where each segment represents a collection of entities. The library provides mechanisms for persisting these segments across distributed storage systems using a bucket-based approach.

Check out the [documentation](https://deepwiki.com/basis-company/sharding.php) or `tests` folder.

## Key Concepts

### Domain Segmentation
- The domain is divided into logical **segments**, each containing a collection of entities
- Segments allow for horizontal partitioning of data

### Persistence Model
- Segment persistence is achieved through **buckets**
- Buckets are distributed across multiple storage backends
- Provides fault tolerance and scalability
- We assume that **each storage node (or database instance) contains at most one logical bucket** (shard). This means:
  - **No need to store a `bucket_id` (or shard key) in the data records**—since the storage location itself implies the bucket.
  - **Sharding is storage-aware**: The system routes requests based on the physical/logical storage, not an attribute in the data.

## Features

- **Shard management**: Easily create and manage data shards
- **Bucket distribution**: Automatic distribution of buckets across storage systems
- **Entity operations**: CRUD operations for entities within segments
- **Storage abstraction**: Support for multiple storage backends
- **Scalability**: Designed to handle large-scale data distribution

## Installation

```bash
composer require basis-company/sharding
```
