#THIS IS A TEMPORARY FIX
#NOTE THERE ARE DUPLICATES IN STACK
#THIS PROBLEM WON"T EXIST ONCE THE STRUCTURES ARE PREPENDED WITH the structure name in the stack creation step.

#takes the stack file and adds the metadata necessary to be a redcap import
root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
#replace this with list from Lexi
#list from Lexi varfile=[]
mapping=pd.read_csv(os.path.join(root_dir,"VariableMap2RedCap.csv"))
mapping.columns=['fromvar','tovar']
mappit=dict(zip(list(mapping.fromvar),list(mapping.tovar)))

stack=pd.read_csv()