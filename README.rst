======================
foglamp-south-playback
======================

FogLAMP South Plugin for playback. `read more <https://github.com/foglamp/foglamp-south-playback/blob/master/python/foglamp/plugins/south/playback/readme.rst>`_

Configuration options explained
===============================

.. code-block:: console

    - 'csvFilename': The name of the CSV file with extension. The location is assumed to be data/ folder.
    - 'headerRow': True if first row of the CSV file contains column headings else leave it unchecked.
    - 'fieldNames': If no header row is present in the CSV file, then provide a comma separated list of column names. The list must contain equal no. of labels as no. of columns in the CSV file.
    - 'readingCols': If readings to be ingested for selective columns and/or with different label, a dict must be provided in the format {old label:new label, ...}. Note that this is useful if header row is present, otherwise we can define the desired label in fieldNames itself.
    - 'timestampFromFile': Select this if a timestamp column is present in the CSV file AND the user_ts will be based upon this instead if system time.
    - 'timestampCol': Mandatory if timestampFromFile is choosen. Provide the timestamp column name from headerRow or fieldNames, as the case may be.
    - 'timestampFormat': Mandatory if timestampFromFile is choosen. Python timestamp format. Default is '%Y-%m-%d %H:%M:%S.%f'. If timestamp format is not known, provide 'None'. In that case, system will try to guess the timestamp and this will be slower.
    - 'ingestMode': Choose one from 'burst' or 'batch'.
    - 'sampleRate': Ingest rate valid for 'batch' mode only and only when timestampFromFile is NOT choosen.
    - 'burstInterval': Interval in ms between two consecutive bursts.
    - 'burstSize': No. of readings sets in one burst.
    - 'repeatLoop': If file is small, then we may want to read it in a loop.

``NOTE: CSV string data with single quote (') and double quotes(") are not supported.``

***********************
Packaging for playback
***********************

This repo contains the scripts used to create a foglamp-south-playback package.

The make_deb script
===================

.. code-block:: console

  $ ./make_deb help
  make_deb help [clean|cleanall]
  This script is used to create the Debian package of foglamp south playback
  Arguments:
   help     - Display this help text
   clean    - Remove all the old versions saved in format .XXXX
   cleanall - Remove all the versions, including the last one
  $


Building a Package
==================

Finally, run the ``make_deb`` command:


.. code-block:: console

    $ ./make_deb
    The package root directory is         : /home/pi/foglamp-south-playback
    The FogLAMP south playback version is : 1.0.0
    The package will be built in          : /home/pi/foglamp-south-playback/packages/build
    The package name is                   : foglamp-south-playback-1.0.0

    Populating the package and updating version file...Done.
    Building the new package...
    dpkg-deb: building package 'foglamp-south-playback' in 'foglamp-south-playback-1.0.0.deb'.
    Building Complete.
    $


The result will be:

.. code-block:: console

  $ ls -l packages/build/
    total 12
    drwxr-xr-x 4 pi pi 4096 Jun 14 10:03 foglamp-south-playback-1.0.0
    -rw-r--r-- 1 pi pi 4522 Jun 14 10:03 foglamp-south-playback-1.0.0.deb
  $


If you execute the ``make_deb`` command again, you will see:

.. code-block:: console

    $ ./make_deb
    The package root directory is         : /home/pi/foglamp-south-playback
    The FogLAMP south playback version is : 1.0.0
    The package will be built in          : /home/pi/foglamp-south-playback/packages/build
    The package name is                   : foglamp-south-playback-1.0.0

    Saving the old working environment as foglamp-south-playback-1.0.0.0001
    Populating the package and updating version file...Done.
    Saving the old package as foglamp-south-playback-1.0.0.deb.0001
    Building the new package...
    dpkg-deb: building package 'foglamp-south-playback' in 'foglamp-south-playback-1.0.0.deb'.
    Building Complete.
    $


    $ ls -l packages/build/
    total 24
    drwxr-xr-x 4 pi pi 4096 Jun 14 10:06 foglamp-south-playback-1.0.0
    drwxr-xr-x 4 pi pi 4096 Jun 14 10:03 foglamp-south-playback-1.0.0.0001
    -rw-r--r-- 1 pi pi 4518 Jun 14 10:06 foglamp-south-playback-1.0.0.deb
    -rw-r--r-- 1 pi pi 4522 Jun 14 10:03 foglamp-south-playback-1.0.0.deb.0001
    $

... where the previous build is now marked with the suffix *.0001*.


Cleaning the Package Folder
===========================

Use the ``clean`` option to remove all the old packages and the files used to make the package.
Use the ``cleanall`` option to remove all the packages and the files used to make the package.
