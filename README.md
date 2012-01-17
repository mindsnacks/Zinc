
# Zinc - Draft 5

#### Design Goals

 - Verifiable & consistent file distributions
 - Transparent and human friendly
 - Versioned
 - S3/CloudFront compatible


#### Terminology

 - Bundle - group of related files. Bundles should have an unique id.
 - Catalog - A server-side listing of bundles that can be downloaded
 - Repo - Client side copies of bundles pulled from remote Catalogs
 - Manifest - describes all files in a bundle
 - Distribution - named version. Sort of like a branch but not really.


## Workflow


#### Set up some assets

     catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |- sounds/
         |- burp.wav

#### Create a Zinc catalog

    $ zinc catalog:create /path/to/catalog
    Created new Zinc catalog 'default' at /path/to/catalog

    $ zinc list /path/to/repo
    0 bundles

     <catalog>/
     |- index.json


#### Create a Zinc bundle in the catalog

	$ cd /path/to/catalog
	$ zinc bundle:update catburp-assets ~/catburp-assets


#### index.json

	{
    	'format' : '1',
    	'bundles' : {
        	'catburp' : [1],
       },
    	'distributions' : {
		}
    }

##### manifest.json

	{
    	"bundle": "fr-Nightlife", 
	    "files": {
	        "Nightlife.js": {
				"sha": "764c6fc121871082e0f0a71c79f1df687b8b741a", 
				"formats": {
					"gzip": {
						"size" : 123123123
					},
				"formats": {
					"gzip": 123123123
					}
				}
			}
	        "audio/acommanderaboire.caf": "0b4aeb04c7c064c034dcbbbd2f8d436095f0342a", 
	        "audio/derencontrerquelquun.caf": "8e0b302f66b17ee6c5ecfc43dc3734b832c42323", 
	        "zincfile": "ae0975988a976bb5ad437585adc5b97d6543499b"
    	}, 
	    "version": 1
	}


# Zinc - Draft 4

#### Set up some assets

     catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |- sounds/
         |- burp.wav


#### Create a Zinc repo

    $ zinc repo:create /path/to/repo
    Created new Zinc repo 'default' at /path/to/repo

    $ zinc list /path/to/repo
    0 bundles

     <repo>/
     |- index.json
     |- repo.json


#### index.json

    {
      'format' : '1',
      'bundles' : {
         'catburp' : {
           'current' : 21,
           'test' : 22,
         'sockpotatoes': {
           'current' : 20,
         }
       }
    }

#### bundles.json

    {
      'format' : '1',
      'bundles' : {
         'catburp' : [20, 21, 22],
         'sockpotatoes': [19, 20],
       }
    }


#### index.json (2)

	{
    	'format' : '1',
    	'bundles' : {
        	'catburp' : [20, 21, 22],
        	'sockpotatoes': [19, 20],
       },
    	'distributions' : {
			'current' : {
				'catburp' : 21,
				'sockpotatoes' : 20,
			},
			'test' : {
				'catburp' : 22,
			},
		},
    }



#### Name and set as current repo

	$ zinc repo:name /path/to/repo 'myrepo'

    $ zinc list myrepo
    0 bundles

	$ zinc repo:use myrepo
	Now using 'myrepo' (/path/to/repo)

    $ zinc list
    0 bundles


#### Create a new bundle in the repo

	$ zinc bundle:create catburp-assets/
	Bundle name [catburp-assets]: 
    Creating bundle 'catburp-assets' in 'myrepo'
	Importing files .. done
	Created v1 of 'catburp-assets'
    Create a 'current' distribution? [Y/n] Y
	Creating 'current' distribution for v1 of 'catburp-assets'

	catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |- sounds/
      |  |- burp.wav
      |- zincinfo
	

#### Create a new bundle in the repo 2

	$ zinc bundle:create catburp-assets
    Creating bundle 'catburp-assets' in 'myrepo'
	Importing files .. done

	$ zinc bundle:update catburp-assets catburp-assets/
	Created v1 of 'catburp-assets'
    Create a 'current' distribution? [Y/n] Y
	Creating 'current' distribution for v1 of 'catburp-assets'

	catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |- sounds/
      |  |- burp.wav
      |- zincfile


#### zincfile

A `zincfile` will be create by default:

	[spec "default"]
	origin = .
	repo = file:///path/to/repo
	bundle = "catburp-assets"


#### zincfile

But you could edit it like this:

    [spec "default" "local"]
    origin = .
	repo = file:///path/to/repo
	bundle = "catburp-assets"

	[spec "mindsnacks-aws"]
	origin = "."
	repo = "s3:///asset-bucket"
	bundle = "catburp-assets"


#### zincinfo

 - `.zincinfo`
 - `.zinc/info`

    [repo "myrepo"]
    	url = file:///path/to/repo

    [bundle]
    	name = 'catburp-assets'


#### zinc repo






# Zinc - Draft 3


#### Design Goals

 - Verifiable & consistent file distributions
 - Transparent and human friendly
 - Versioned
 - S3/CloudFront compatible


#### Terminology

 - Repository (or Repo) - top level zinc concept. Contains everything else.
 - Bundle - group of files. A repo contains 1 or more bundles.
 - Manifest - describes all files in a bundle
 - Distribution - named version. Sort of like a branch but not really


## Workflow

#### Set up some assets

     catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |- sounds/
         |- burp.wav

#### Create a Zinc repo

    $ zinc init catburp
    Created new Zinc repo at ./myrepo

    $ zinc list myrepo
    0 bundles


#### Add assets to the repo

    $ zinc update catburp ~/catburp-assets
    Bundle 'catburp' does not exist. Create (Y/n)? Y
    Creating version 1 of bundle 'catburp' ..
    Importing files ..
    Updating index
	Done
    
    $ zinc list myrepo
    1 bundle:
      catburp: 1

    $ zinc list myrepo catburp
      catburp-1:
        images/cat1.jpg
        images/cat2.jpg
        sounds/burp.wav

    $ zinc list myrepo catburp@1
      images/cat1.jpg
      images/cat2.jpg
      sounds/burp.wav

#### Verify the repo

    $ zinc verify myrepo
    Verifying all bundles .. ok!
    Orphaned objects: 0

#### Publish the repo

    $ zinc publish myrepo s3://myassets
    Publishing all files .. done!

    # zinc activate myrepo catburp@1
    This will set catburp 1 to you activate distribution. You sure bro (Y/n)? Y
    Updating index .. done!

#### Update the repo

     catburp-assets/
      |- images/
      |  |- cat1.jpg
      |  |- cat2.jpg
      |  |- cat3.jpg
      |- sounds/
         |- burp.wav

    $ zinc update catburp ~/catburp-assets
    Creating version 2 of bundle 'catburp' ..
    1 file added, 0 files modified, 0 files removed

    $ zinc list myrepo
    1 bundle:
      catburp: 1, 2

    $ zinc publish myrepo s3://myassets
    Publishing all files .. done!

    # zinc activate myrepo catburp@2
    This will set catburp 2 to you activate distribution. You sure bro (Y/n)? Y


## Zinc Repo


     <repo>/
     |- index.json
     |- zinc/
     |  |- config
     |  |- bundles
     |- objects/
     |  |- burp@3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- burp@1232854d83448fed0b95141d6da0c2c3a85fbe21.wav
     |  |- cat1@3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2@a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- cat2@99999999999999999ed0546fe871ef486321ce49.jpg
     |  |- potatoes@1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks@f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |  |- socks@888888888888881a140eb3c3ad8fef9ce904ab4a.pdf
     |- manifests/
     |  |- catburp-21.json
     |  |- catburp-21.json
     |  |- catburp-22.json
     |  |- sockpotatoes-19.json
     |  |- sockpotatoes-20.json
     |- archives/
        |- catburp-21.zip
        |- catburp-21.zip
        |- catburp-22.zip
        |- sockpotatoes-19.zip
        |- sockpotatoes-20.zip



#### index.json

    {
      'zinc_format' : '1',
      'bundles' : {
         'catburp' : {
           'current' : 21,
           'test' : 22,
         'sockpotatoes': {
           'current' : 20,
         }
       }
    }


#### zinc/config

    [core]
	    ignorecase = true
    [remote "s3"]
    	url = s3://content


#### zinc/bundles

    catburp: 20, 21, 22
    sockpotatoes: 19, 20



#### catburp-21.json

    {
       'versionkk' : 21,
       'files' : {
          'images/cat1.jpg' : 'cat1@3716da9df43aa8a45fc40432706403e5fab6db2d.jpg',
          'images/cat2.jpg' : 'cat2@a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg',
          'sounds/burp.wav' : 'burp@3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav',
        },
    }


#### Client

1) Fetch <repo>/index.json
2) Look up desired bundle, distribution
3) Download, Download, Download
4) Verify when finished downloaded
5) Update current manifest (symlink or otherwise)







----


# Zinc - Draft 2


#### Design Goals

 - Verifiable & consistent file distributions
 - Transparent and human friendly
 - Versioned, with minor and major updates
 - S3/CloudFront compatible


#### Terminology

 - Repository (or Repo) - top level zinc concept. Contains everything else.
 - Bundle - group of files. A repo contains 1 or more bundles.

## Zinc Repo

     <repo>/
     |- info.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- archives/
     |  |- catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.zip
     |  |- sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.zip
     |- manifests/
        |- catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.json
        |- sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.json


     <repo>/
     |- info.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- archives/
     |  |- catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.zip
     |  |- sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.zip
     |- manifests/
        |- catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.json
        |- sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.json

     |- bundles/
        |- catburp/
           |- manifest.json
           |- archive.zip

     |- bundles/
        |- catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.json
        |- catburp-20.json
        |- sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.json



     <repo>/
     |- index.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- bundles/
        |- catburp-20.json
        |- sockpotatoes-20.json
        |- sockpotatoes-21.json
        |- archives/
           |- catburp-20.zip
           |- sockpotatoes-20.zip
           |- sockpotatoes-21.zip

     <repo>/
     |- index.json
     |- objects/
     |  |- 3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21
     |  |- 3716da9df43aa8a45fc40432706403e5fab6db2d
     |  |- a5a5fd2dd22f9081bed0546fe871ef486321ce49
     |  |- 1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0
     |  |- f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a
     |- bundles/
        |- catburp/
           |- catburp-20.json
           |- catburp-20.zip
           |- catburp-21.json
           |- catburp-21.zip
        |- sockpotatoes-20.json
        |- sockpotatoes-21.json
        |- archives/
           |- catburp-20.zip
           |- sockpotatoes-20.zip
           |- sockpotatoes-21.zip


     <repo>/
     |- index.json
     |- objects/
     |  |- 3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21
     |  |- 3716da9df43aa8a45fc40432706403e5fab6db2d
     |  |- a5a5fd2dd22f9081bed0546fe871ef486321ce49
     |  |- 1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0
     |  |- f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a
     |- bundles/
         |- catburp-20.json
         |- catburp-21.json


#### index.json

    {
      'zinc_format' : '1',
      'bundles' : {
         'catburp' : {
           'current' : 20,
           'all' : [1,2,3,4,5,6,8,20],
         'sockpotatoes' [21],
       }
    }


    {
      'zinc_format' : '1',
      'bundles' : {
         'catburp' : 21,
         'sockpotatoes' : 20,
       }
    }




#### info.json

    {
      'zinc_format' : '1',
      'zinc_repo' : {
         'versions' : [20, 21],
         'bundles' : {
            20 : {
              'catburp' : {
                'manifest' : 'f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a',
                'archive'  : '3716da9df43aa8a45fc40432706403e5fab6db2d',
               },
               'sockpotatoes' : {
                 'manifest' : 'f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a',
                 'archive'  : '3716da9df43aa8a45fc40432706403e5fab6db2d',
                }
             },
            21 : {
              'catburp' : {
                'manifest' : 'f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a',
                'archive'  : '3716da9df43aa8a45fc40432706403e5fab6db2d',
               },
               'sockpotatoes' : {
                 'manifest' : 'f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a',
                 'archive'  : '3716da9df43aa8a45fc40432706403e5fab6db2d',
                }
             }
          }
       }
    }

#### index.json

    {
      'zinc_format' : '1',
         'bundles' : {
            'catburp' : 'bundles/catburp-20.json',
            'sockpotatoes' : 'bundles/sockpotatoes-21.json',
          }
       }
    }

#### manifest: catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.json

    {
       'rev' : 20,
       'archive' : 'archives/catburp-20.zip',
       'files' : {
          'images/cat1.jpg' : 'objects/cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg',
          'images/cat2.jpg' : 'objects/cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg',
          'sounds/burp.wav' : 'objects/burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav',
        },
        'bundles' : {
          'sockpotatoes' : 'bundles/sockpotatoes-20.json',
        } 
    }


#### manifest: sockpotatoes+51b8d94f26703b7d56cea32c93b8e79971e756d9.json

    {
       'version' : 20
       'files' {
          'potatoes.doc' : 'potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc',
          'socks.pdf'    : 'socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf',
        }
    }

----


# Zinc - Draft 1

#### Design Goals
 - Verifiable & consistent file distributions
 - Transparent and human friendly
 - Versioned, with minor and major updates
 - S3/CloudFront compatible


## Example

Say we want to distribute these set of assets:

     src/
     |- images/
     |  |- cat1.jpg
     |  |- cat2.jpg
     |- sounds/
     |  |- burp.wav

#### Create initial Zinc dist

    $ zinc update src dst

#### Output

     dst/
     |- info.json
     |- objects/
     |  |- images/
     |  |  | - cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |  | - cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- sounds/
     |      |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- versions/
         |- 1/
            |- manifest.json
    
#### info.json

    {
      'zinc_format' : '1',
      'url' : 'http://localhost/data/,
    }
      
#### manifest.json

    {
       'version' : '1.1',
       'files' {
          'images/cat1.jpg' : '3716da9df43aa8a45fc40432706403e5fab6db2d',
          'images/cat2.jpg' : 'a5a5fd2dd22f9081bed0546fe871ef486321ce49',
          'sounds/burp.wav' : '3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21',
        }
    }


### Update a file

Now say cat1.jpg was updated. We update the zinc dist the same way:

    $ zinc update <src> <dst>

A new copy of cat1.jpg is put in the repo. The manifest is also updated with the new SHA and minor version.

#### Output

     dst/
     |- info.json
     |- objects/
     |  |- images/
     |  |  | - cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |  | - cat1+fca119db2818c61b859b1bac96f35f83533214fe.jpg
     |  |  | - cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- sounds/
     |      |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- versions/
         |- 1/
            |- manifest.json
       

#### versions/1/manifest.json

    {
       'version' : '1.2',
       'files' {
          'images/cat1.jpg' : 'fca119db2818c61b859b1bac96f35f83533214fe',
          'images/cat2.jpg' : 'a5a5fd2dd22f9081bed0546fe871ef486321ce49',
          'sounds/burp.wav' : '3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21',
        }
    }


### Cleaning

By default, older files aren't removed immediately. To remove them:

    $ zinc clean <dst>


### Adds and Deletes

If at lest one file is added or removed, a new **major version** is created.

     src/
     |- help.html
     |- images/
     |  |- cat1.jpg
     |- sounds/
     |  |- burp.wav

Here `cat2.jpg` was deleted and `help.html` was added. If we `zinc update` again, a new version will be created

     dst/
     |- info.json
     |- objects/
     |  |- help+6145ea18fb1fc08110ba4622b5481b5ef74d2cd5.html
     |  |- images/
     |  |  | - cat1+fca119db2818c61b859b1bac96f35f83533214fe.jpg
     |  |  | - cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- sounds/
     |      |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- versions/
         |- 1/
         |  |- manifest.json
         |- 2/
            |- manifest.json

`versions/1/manifest.json` is unchanged

#### versions/2/manifest.json

    {
       'version' : '2.1',
       'files' {
          'images/cat1.jpg' : 'fca119db2818c61b859b1bac96f35f83533214fe',
          'sounds/burp.wav' : '3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21',
          'help.html'       : '6145ea18fb1fc08110ba4622b5481b5ef74d2cd5',
        }
    }


## FAQs

#### Why doesn't Zinc do (insert VCS feature)?

Zinc is **not** a version control system. 

#### Why JSON?

JSON has become a de-facto interchange format, yada, yada, and is human-friendly.


----

# Attic, random notes

#### other layout ideas, draft 2

1)

     <repo>/
     |- info.json
     |- objects/
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- bundles/
        |- catburp.json


2)

     <repo>/
     |- info.json
     |- objects/
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- bundles/
        |- catburp/
           |- manifest.json
           |- catburp.zip

3)
    
     <repo>/
     |- info.json
     |- objects/
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |- bundles/
        |- catburp.zip
        |- catburp.zip.zsync
        |- catburp/
           |- manifest.json






     <repo>/
     |- info.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- bundles/
        |- catburp.zip
        |- catburp.zip.zsync
        |- catburp.json
        |- sockpotatoes.zip
        |- sockpotatoes.zip.zsync
        |- sockpotatoes.zip.json

     <repo>/
     |- info.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- bundles/
        |- catburp+20.zip
        |- catburp+20.zip.zsync
        |- catburp+20.json
        |- sockpotatoes+20.zip
        |- sockpotatoes+20.zip.zsync
        |- sockpotatoes+20.zip.json


     <repo>/
     |- info.json
     |- objects/
     |  |- burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav
     |  |- cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg
     |  |- cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg
     |  |- potatoes+1fb4cd01299779e4c48f0a49a7bd4f3b58a03be0.doc
     |  |- socks+f78200b67a1ee01a140eb3c3ad8fef9ce904ab4a.pdf
     |- bundles/
        |-20/
          |- catburp.zip
          |- catburp.zip.zsync
          |- catburp.json
          |- sockpotatoes.zip
          |- sockpotatoes.zip.zsync
          |- sockpotatoes.zip.json



    {
      'zinc_format' : '1',
      'zinc_repo' : {
         'version' : 20,
         'bundles' : ['catburp', 'sockpotatoes'],
       }
    }

