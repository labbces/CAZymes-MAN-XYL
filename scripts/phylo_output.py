# IMPORTS
import argparse
from ete3 import Tree, TreeStyle

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument('-i','--input', help="Treefile",type=str,required=True, metavar="")
parser.add_argument('-f','--filename', help="prefix of output files",type=str,required=True, metavar="")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-n','--ninja', action="store_true",help="newick file from ninja")
group.add_argument('-iqu','--iqultrabootstrap', action="store_true", help="Processe using fastbootstrap")
group.add_argument('-iqa','--iqalrt', action="store_true",help="Processe using alrt")
args = parser.parse_args()


# Loading TreeFile
t = Tree(args.input, format=1)

#Getting midpoint outgroup for tree and rooting tree
midpoint_outgroup = t.get_midpoint_outgroup()
t.set_outgroup(midpoint_outgroup)

# Handling with double support values

if not args.ninja:
    for node in t.traverse("postorder"):
        if not node.is_leaf():
            try:
                if args.iqultrabootstrap:
                    node.support = float(node.name.split("/")[0]) # value of the support using fastbootstrap
                elif args.iqalrt:
                    node.support = float(node.name.split("/")[1])# value of the support using alrt
            except:
                continue
            
# Setting names equal IDs
for node in t.iter_leaves():
    node.name = node.name.split("_")[0]

# Tree description
t.describe()

# Setting Tree Style and Visualization
ts = TreeStyle()
ts.show_leaf_name = True
ts.show_scale = True
ts.show_branch_length = True
ts.show_branch_support = True
#t.show(tree_style=ts)
try:
    if args.iqultrabootstrap:
        t.render(f"{args.filename}_withfastbootstrap.pdf", tree_style=ts)
        t.render(f"{args.filename}_withfastbootstrap.svg", tree_style=ts)
    elif args.iqalrt:
        t.render(f"{args.filename}_withalrt.pdf", tree_style=ts)
        t.render(f"{args.filename}_withalrt.svg", tree_style=ts)
except:
    pass


# Writing the tree in newick format
try:
    if args.iqultrabootstrap:
        t.write(format=0, outfile=f"{args.filename}_withfastbootstrap.nwk") 
    elif args.iqalrt:
        t.write(format=0, outfile=f"{args.filename}_withalrt.nwk")
except:
    pass