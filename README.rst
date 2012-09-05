Zinc
====

Zinc is asset distribution system for designed mobile apps. Common use cases
are for downloading content purchased through an in-app-purchase mechanism, or 
to avoid packaging large files inside your app package if they may not be
needed.

The central concept in Zinc is a "bundle", which is just a group of assets. For
example, a bundle could be a level for a game, which includes spritesheets,
background music, and metadata.

Zinc bundles are also versioned, which means they can be updated. For example,
if you wanted to change the background music in a game, you can do that by
pushing a new version of that bundle.

Finally, Zinc also has a notion of "distributions" for a bundles, which are
basically just tags. This allows clients to track a tag such as `master`
instead of the version directly. Then whenever the `master` distribution is
updated, clients will automatically download the changes.

Feature Highlights
------------------

- Files are tracked by content. If the same file exists in multiple bundles or
versions, it is only stored once.  
- "CDN Compatible" meaning that a Zinc catalog can be hosted on a dumb file server.

Status
------

Zinc is in the early development stage should be consider 'alpha' at
best.

Clients
-------

- `Objective-C`_ 

.. _`Objective-C`: https://github.com/mindsnacks/Zinc-ObjC/

Credits
-------

- `Distribute`_
- `Buildout`_
- `modern-package-template`_

.. _Buildout: http://www.buildout.org/
.. _Distribute: http://pypi.python.org/pypi/distribute
.. _`modern-package-template`: http://pypi.python.org/pypi/modern-package-template

License
-------

Zinc is distributed under a BSD-style license.

    Copyright (c) 2011-2012 MindSnacks (http://mindsnacks.com/)
        
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
