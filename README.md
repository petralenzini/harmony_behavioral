# harmony_behavioral

### functions.py - commonly used functions
### inventory.py
###### harmonizes subject demographics (basic) and catalogs files in which they appear
###### crawls through unedited files upload by collaborators
###### first it harmonizes sex, site, case/control status, race, and age
###### then it takes an inventory of files (changing .txt to .csv for NDAR subjects) to see what we know about structure overlap
###### output was uploaded to BOX
### stack4studies.py 
###### full harmonization of NDA structures and timepoints
### varDistributions.py
###### sanity checks on all variables distributions.  See https://docs.google.com/presentation/d/1ez8M1i5jRNGobHz5cWR0bAVfRQ4uYE3APWPYMQLn_-w/edit#slide=id.g2625181301d_0_287 for issues that were unresolved as of Nov. 27, 2023
### makeDictionary.py
###### takes the harmonized stack of data and creates the data dictionary

###### TO DO: still need to reconcile the structures in the inventory to those in Leanard Tozzi's paper.  We want the exact names of files corresponding with each of those in Table so that we can use the 'machine readable' version of his table to loop through the mapped structures.  


