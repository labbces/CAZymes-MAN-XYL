import argparse

'''                                 Description
        Replace characteres in a multifasta like file to avoid parsing problemas. Reads line by line and write a new file with the changes.
'''
# argparse to handle variables in terminal
parser = argparse.ArgumentParser()
parser.add_argument("sequenceFile", help="input file")
parser.add_argument("newsequenceFile", help="output file")
parser.add_argument("undesired_character", help="character to be changed")
parser.add_argument("desired_character", help="character that will replace undesired character")

args = parser.parse_args()

# Setting variables
old_char = args.undesired_character
new_char = args.desired_character

# Reading files safely
try:
    original_seq_handler = open(args.sequenceFile, 'r')
except:
    print("File", args.sequenceFile, "cannot be open")
    exit()

try:
    transformed_sequence = open(args.newsequenceFile, 'w')
except:
    print("File", args.newsequenceFile, "cannot be open")

# Writing new file with the changes
for line in original_seq_handler:
    if line.startswith('>'):
        line = line.replace(old_char, new_char)  # Replacing
        transformed_sequence.write(line)
    else:
        transformed_sequence.write(line)
transformed_sequence.close()