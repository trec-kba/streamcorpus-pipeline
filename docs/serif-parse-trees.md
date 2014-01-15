Serif's dependency parser generates trees that are stored in [Token.dependency_path](https://github.com/trec-kba/streamcorpus/blob/master/if/streamcorpus-v0_3_0.thrift#L423) and related fields.

Each dependency path shows the complete path through the constituent
parse tree to get from a word to the word it depends on.  This path
consists of two parts: walking up the tree from the source word to the
common ancestor node; and then walking down the tree to the word that
the source word depends on.  These are expressed as follows:

<pre>
   PATH     := PATH_UP CTAG PATH_DN
   PATH_UP  := "/" | "/" CTAG PATH_UP
   PATH_DN  := "\" | "\" CTAG PATH_DN
   CTAG     := S | NP | PP | VP | etc...
</pre>

Where PATH_UP is the path up from the source word to the common
ancestor, the "CTAG" in the "PATH" expansion is the constituent tag
for the common ancestor, and PATH_DN is the path back down to the word
that the source word depends on.  The "root" node (the one whose
"parent_id" is -1) will always have an empty PATH_DN.

Here's an example sentence:

<pre>
> 0 Comment    -1  /NPA/NP\
> 1 on          0  /PP/NP\NPA\
> 2 Third       3  /NPA\
> 3 Baby        1  /NPA/PP\
> 4 for         0  /PP/NP\NPA\
> 5 Umngani     4  /NPA/PP\
> 6 by          0  /PP/NP\NPA\
> 7 ALLE        6  /NPP/PP\
</pre>

And here's the corresponding parse tree (with constituent tags, but
*not* part of speech tags):

<pre>
   (NP (NPA Comment)
       (PP on (NPA Third Baby))
       (PP for (NPA Umnagi))
       (PP by (NPP ALLE)))
</pre>

To take an example, if we start at the word "for" (word 4), and trace 
the path to the word it depends on (word 0), then we go up the tree from 
"for->PP->NP", and then back down the tree from "NP->NPA->Comment". 
Putting those paths together, we get "/PP/NP\NPA", which is indeed the 
dependency label shown.

To reconstruct the parse tree that it came from, we can start with the 
root node ("Comment") -- based on its PATH_UP, we have:

<pre>
     (NP (NPA Comment))
</pre>

Then we can add in the words that are dependent on it (words 1, 4, and 
6), to get:

<pre>
     (NP (NPA Comment) (PP on) (PP for) (PP by))
</pre>

(Note that the complete PATH_DN is actually more information that we 
really need to reconstruct the tree -- in particular, all we really need 
to know is how long PATH_DN is -- i.e., "how high" to attach the 
dependent).  Moving on, we attach the words that are dependent on the 
prepositions to get:

<pre>
   (NP (NPA Comment)
       (PP on (NPA Baby))
       (PP for (NPA Umnagi))
       (PP by (NPP ALLE)))
</pre>

And finally we attach "Third" to "Baby" using the very short path 
"/NPA\" to get back to the original tree:

<pre>
   (NP (NPA Comment)
       (PP on (NPA Third Baby))
       (PP for (NPA Umnagi))
       (PP by (NPP ALLE)))
</pre>
