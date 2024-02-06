WHOAMI=$(whoami)
echo $WHOAMI

BIBLIO="./biblio.tex"

if test -f "$BIBLIO"; then
	echo "biblio.tex found in current path :: continue with make"
else
	echo "biblio.tex NOT found in current path :: checking for library.bib in default Dropbox path"
	POSSIBLE_LIBPATH=`find ~/Dropbox -type f -name "library.bib" | grep "W-Lab/Bibliography-Mendeley" | head -1`
	echo "Trying in $POSSIBLE_LIBPATH"
	LIBPATH=$POSSIBLE_LIBPATH
	echo $LIBPATH
	if test -f "$LIBPATH"; then
		echo "library.bib found in default Dropbox path :: creating biblio.tex (in current path)"
		touch biblio.tex && echo "\\\bibliographystyle{abbrv}" > biblio.tex && echo "\\\bibliography{$LIBPATH}" >> biblio.tex
		echo "biblio.tex created successfully !!"
	else 
		echo "library.bib NOT found in default Dropbox path :: searching in other locations within Dropbox folder"
		DRPBXLIBPATH=$(find  /Users/$WHOAMI/Dropbox/ -name "library.bib" 2> /dev/null)
		echo "library.bib found in $DRPBXLIBPATH"!
		read -p 'do you want to use this path to create biblio.tex? (y/n) : ' upath
		if [ $upath == "y" ]; then
			echo "creating biblio.tex (in current path)"
			touch biblio.tex && echo "\\\bibliographystyle{abbrv}" > biblio.tex && echo "\\\bibliography{$DRPBXLIBPATH}" >> biblio.tex
			echo "biblio.tex created successfully !!"
		else
			read -p 'do you want to search in other locations? (y/n) : ' upath
			if [ $upath == "y" ]; then
				echo "library.bib NOT found within Dropbox :: searching in other places"
				ALTLIBPATH=$(find  /Users/$WHOAMI/ -name "library.bib" 2> /dev/null)
				echo "library.bib found in $ALTLIBPATH"!!
				read -p 'do you want to use this path to create biblio.tex? (y/n) : ' upath
				if [ $upath == "y" ]; then
					echo "creating biblio.tex (in current path)"
					touch biblio.tex && echo "\\\bibliographystyle{abbrv}" > biblio.tex && echo "\\\bibliography{$ALTLIBPATH}" >> biblio.tex
					echo "biblio.tex created successfully !!"
				else
					echo "aborting !!"
				fi
			else
				echo "terminating search !!"
			fi
		fi
		
	fi
fi
