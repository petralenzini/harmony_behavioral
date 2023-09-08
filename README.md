This is a README
# harmony_behavioral

# inventory.py

# inventory.py crawls through unedited files upload by collaborators
# first it harmonizes sex, site, case/control status, race, and age
# then it takes an inventory of files (changing .txt to .csv for NDAR subjects) to see what we know about structure overlap
# Then it creates a few plots describing the age, sex, race, and case/control status by study so that we can see whats missing
# the output is the inventory table, plots, and a summary table by site (all uploaded to BOX next to the site specific tables)

# TO do: compare the structures in the inventory to those in Leanard Tozzi's paper.  We want the exact names of files corresponding with each of those in Table 
#        so that we can use the 'machine readable' version of his table to loop through the mapped structures.  

# AFTER THAT: for each structure in Table 3, match the variables in the 4 sources using names (if NDA source) or fuzzy mapping (STACT) using NLP.
	      after the variables in each structure are mapped to one another, do distributions by site to make sure we're not missing unit changes or value shifts

