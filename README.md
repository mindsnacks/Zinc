# Zinc

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
