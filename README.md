Pivoting in Pig
===============

###An example of how to create an Excel-like pivot table in Apache Pig

This an embedded Pig script based off [this Stack Overflow question](http://stackoverflow.com/questions/22416883/advanced-pivot-table-in-pig/22469696#22469696). I've included the example data the questioner uses. It's extensible to more factor levels than the sample data provides, but some modifications need to be done before it can pivot on any number of rows or columns.

####Local usage

To test it out, clone this repo and run `pig -x local pivot.py`, assuming you have a Jython-compatible version of Pig installed on your local machine.
