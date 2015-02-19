SQuangLe
========

Overview
--------

SQuangLe is a C++ MySQL client library built on top of
[WebScaleSQL](http://webscalesql.org/)'s C client library. It does not require
a WebScaleSQL server - WebScaleSQL, MySQL, MariaDB, and Percona Server should
all work fine.

Current Status
--------------

**THIS IS AN EARLY PREVIEW RELEASE**

 - Expect the API to change
 - Documentation is not ready for release
 - Automated tests are not included - they exist, however they require Facebook
   infrastructure to run, so releasing them seemed more confusing than useful

In particular, we plan to move everything from the various namespaces to
'squangle'.

We have released it at this early stage as it is a dependency of other Facebook
projects.

Features
--------

 - Object-oriented API
 - Query builder with automatic escaping
 - Asynchronous query execution

License
-------

SQuangLe is BSD-licensed. We also provide an additional patent grant. Please
see the LICENSE and PATENTS files.

Contributing
------------

Please see CONTRIBUTING.md
