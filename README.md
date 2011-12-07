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



#### manifest: catburp+cd9e290639a98fbbb797685f0c99f3c977692bc9.json

    {
       'version' : 20
       'files' {
          'images/cat1.jpg' : 'cat1+3716da9df43aa8a45fc40432706403e5fab6db2d.jpg',
          'images/cat2.jpg' : 'cat2+a5a5fd2dd22f9081bed0546fe871ef486321ce49.jpg',
          'sounds/burp.wav' : 'burp+3e36854d83448fed0b9c4a6d6da0c2c3a85fbe21.wav',
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
