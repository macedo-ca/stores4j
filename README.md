# stores4j - Unified text and binary storage

The stores4j library unifies reading and writing text and binary files from:

- File-system 	(file:)
- In-memory 	(mem:)
- Windows/Samba network-folder (smb:)
- AWS S3 		(s3:)
- Oracle database table with CLOB/BLOB/FILE field (db:)
- Redis 		(redis:)

The framework has three main interfaces to use, BinStores for reading/writing binary files, TextStores for reading/writing text-files and JsonStore for reading/writing JSON data. JsonStore is using TextStores as underlying persistence and TextStores can also use any BinStores for underlying persistence.

# Installation

1. Install stores4j library
[Download](https://github.com/macedo-ca/stores4j/releases) and add stores4j-[version].jar in your java project lib folder. If you want to use s3, redis, or samba 

2. Add dependencies
Add in required dependencies specific to the storage you want to use, here are the gradle dependencies:

```
// UTILITY - REQUIRED
compile group: 'commons-io', name: 'commons-io', version: '2.5'
// LOGGING - REQUIRED
compile 'org.slf4j:slf4j-api:1.7.21'
compile group: 'org.slf4j', name: 'slf4j-simple', version: '1.7.22'
// JSON converters - REQUIRED
compile 'com.google.code.gson:gson:2.8.0'
compile group: 'com.googlecode.json-simple', name: 'json-simple', version: '1.1.1'
// Windows/Samba network-folder - OPTIONAL - Needed for smb: URIs
compile group: 'jcifs', name: 'jcifs', version: '1.3.17'
// AWS S3 - OPTIONAL - Needed for s3: URIs
compile group: 'com.amazonaws', name: 'aws-java-sdk-s3', version: '1.11.112'
```


# Usage

## Storage options and URI patterns

To make a connection to a store storage location, a single string parameter (URI) is needed. The URI pattern is different for the various storage options.

Any storage URI can optionally have a _store:_ prefix, which is ignored. This is useful if using the store4j framework together with other frameworks, or if you want to indicate that a string configuration is inteded to be used with the stores4j framework.

### File-system (file:)

```
file:[file-system-path]
```

**file-system-path** is the local OS file-system path in _Java format_, i.e. windows path needs to using forward slashes. See [Java File SDK documentation for more details](https://docs.oracle.com/javase/8/docs/api/java/io/File.html).

### Memory (mem:)

```
mem:[memory-space-id]
```

**memory-space-id** is a unique id of the memory space to use. Once created the memory space can be accessed again using the same id. Since the data is stored in java memory, be careful about how much data you put in!

### Samba/Windows server (smb:)

```
smb://[domain];[username]:[pasword]@[hostname]/[path]
```

**hostname** is the hostname of the Windows or Samba server
**domain** is the domain recognized by the server that is being connected. **user** is a valid domain user and **password** is the password of that user.
**path** is the path on the server that is being used for storage.

Please note that unlike windows login scenarios, the domain is seperated from the username using semi-colon (;).

### AWS S3 (s3:)

```
s3:[accessKey]:[secretKey]@[region-code]/[bucket][/folder (OPTIONAL)]
```

**accessKey** and **secretKey** are your AWS credentials used to connect to AWS S3. Make sure that the user has access to S3 and the bucket.
**region-code** is the [AWS region to connect to, see AWS documentation for complete listing](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions)
**bucket** is the logical name of the S3 bucket to use. This is the short-form name, not the GUID.
**folder** (OPTIONAL) is the sub-folder within the bucket to use for storage.

### Oracle Database table (db:)

```
db:[file-group]:[jdbc-connnection-string]
```

**file-group** is the logical group of binary files within the database table. This allows for a single database table to used for multiple storage purposes.
**jdbc-connnection-string** is a standard Oracle JDBC connection string

Here is the DDL for the database table:
```
CREATE TABLE BINARIES (
	"FID" 		VARCHAR2(255) 			NOT NULL ENABLE, 
    "FGROUP" 	VARCHAR2(255) 			NOT NULL ENABLE, 
    "FFOLDER" 	VARCHAR2(4000 BYTE), 
    "FNAME" 	VARCHAR2(1000 BYTE), 
    "FCREATED" 	TIMESTAMP, 
    "FUPDATED" 	TIMESTAMP, 
    "FDATA" 	BLOB, 
    "FSIZE" 	NUMBER(19,0), 
     CONSTRAINT "BINARIES_PK" PRIMARY KEY ("FID", "FGROUP")
)
```

### Redis (redis:) (TextStores ONLY)

```
redis:[hostname]?password=[password]&port=[port]#[hash-key]
```

**hostname** is the redis-server hostname. 
**password** is the redis server password. 
**port** (OPTIONAL) is the redis service port number (6379 by default).
**hash-key** is the hash root-key to perform hset and hget commands for. 

The redis TextStore instance is blazingly fast, and uses [hash commends](https://redis.io/commands#hash) to read and write entries under a specific key in a redis database.

This store requires additional installation to work. To use this storage option, [download and add in util1 library](https://github.com/macedo-ca/util1/releases) and add the jedis dependency:

```
// REDIS
compile group: 'redis.clients', name: 'jedis', version: '2.9.0'
```

## Securing configuration

To protect passwords the framework allows for two additional options in URIs:

- Default credentials  (e.g. _s3:ca-central-1/atestbucket/_)
- Named secret references (e.g. _s3:SECRET(mys3token):ca-central-1/atestbucket/_)

### Default credentials

Default credentials can be setup with BinStores and TextStores instances so that the secrets do not need to be provided within the URIs, but instead simply omitted. 

To set default credentials, call either of:

- BinStores.setDefaultCredentials(String storeType, String credentials)
- TextStores.getBinStores().setDefaultCredentials(String storeType, String credentials)

The credential string passed in is specific to the type of store.

### Named secret references

Examples:
```
smb://SECRET(mydomaincredentials)@windowsserver.mydomian.com/thepath/of/wisdom/
s3://SECRET(mys3tokens):ca-central-1/path/of/wisdom/
redis:inst123.ec2.cloud.redislabs.com?password=SECRET(redispwd)&port=10502#path:of:wisdom
```

As seen above, any URI can include a reference to a secret instead of hard-coded secret using the _SECRET([secretname])_ syntax. The secrets are retrieved when the URI is parsed, and the sources for the secrets needs to be registered with the factory instance ahead of time with the stores instance, so either:

- BinStores.addSecretsSource(Function<String,String>)
- TextStores.addSecretsSource(Function<String,String>)

## Using BinStores
To use stores4j framework, create a new instance of BinStores class. The BinStores factory class creates BinStore instances, where a BinStore represents access to a storage location. BinStore instance are created by calling getStore(String) method, passing in the full URI of the desired store. The prefix of the URI determines the type of store to be created. 
```java
BinStores binStores=new BinStores();
BinStore fileStore = binStores.getStore("file://C:/temp/");
```
The BinStore interface is abstract and allows identical methods regardless of backing storage technology. In summary, the features are:

1. List / Filter - Get a listing / Filtered listing of entry IDs
2. Visit - visit all / filtered-list of entries
3. Read / Write bytes
4. Read / Write stream
5. Bulk operations - Copy / Synchronize (one-way)

Here is a comprehensive java example, using each feature:

```java
// 0. Create store instance
BinStores binStores=new BinStores();
BinStore fileStore = binStores.getStore("file://C:/temp/");

// 1. List / Filter - Get a listing / Filtered listing of entry IDs
for(String id : fileStore.list()) System.out.println(id);
for(String id : fileStore.filter("*.txt")) System.out.println(id);

// 2. Visit - visit all / filtered-list of entries
fileStore.forEach( entry -> {
	try {
		System.out.println(entry.getID() +", size " + entry.getContent().length + " bytes");
	} catch (IOException e) {
		e.printStackTrace();
	}
} );
fileStore.forEachFiltered( entry -> {
	try {
		System.out.println(entry.getID() +", size " + entry.getContent().length + " bytes");
	} catch (IOException e) {
		e.printStackTrace();
	}
} , "*.txt");

// 3. Read / Write bytes
byte[] pictureData=fileStore.item("test.jpg").getContent();
fileStore.item("test_jpg_base64_encoded.txt").setContent(Base64.getEncoder().encode(pictureData));

// 4. Read / Write streams
try(InputStream is=fileStore.item("test.jpg").getContentStream()){
	// The second optional parameter is length, if known, which can optimize large data transfers
	fileStore.item("test_copy.jpg").setContentStream(is, null); 
}

// 5. Bulk operations - Copy / Replicate
BinStore fileStoreCopy = binStores.getStore("file://C:/temp/copyOfTemp/");
fileStore.copyTo(fileStoreCopy);
// The difference between copy and replica is that replica removes any entries in the target that is not found in the source.
BinStore fileStoreReplica = binStores.getStore("file://C:/temp/replicaOfTemp/");
fileStore.replicateTo(fileStoreReplica);
```
