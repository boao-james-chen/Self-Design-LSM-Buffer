whoami := $(shell whoami)
target := paper
latex := pdflatex
bibtex := bibtex

.PHONY: default
default: all 

.PHONY: all
all:
	$(MAKE) checkbib $(target).pdf cleanmost

checkbib: 
	sh ./get_bib.sh

%.pdf: %.tex $(wildcard *.tex) 
	$(latex) $<
	$(bibtex) $*
	$(latex) $<
	$(latex) $<

one:
	$(latex) $(target).tex

bib:
	$(bibtex) $(target)

.PHONY: clean
clean:
	make cleanmost
	rm -f $(patsubst %,%.fls,$(target)) \
	$(patsubst %,%.out,$(target)) \
	$(patsubst %,%.fdb_latexmk,$(target)) \
	$(patsubst %,%.synctex*,$(target)) \
	$(patsubst %,%.synctex.gz,$(target))

.PHONY: cleanmost
cleanmost:
	rm -f *~ *.bak \
	$(patsubst %,%.dvi,$(target)) \
	$(patsubst %,%.log,$(target)) \
	$(patsubst %,%.aux,$(target)) \
	$(patsubst %,%.bbl,$(target)) \
	$(patsubst %,%.blg,$(target)) \
	$(patsubst %,%.lox,$(target)) \
	$(patsubst %,%.upa,$(target)) \
	$(patsubst %,%.upb,$(target)) \
	$(patsubst %,%.xcp,$(target))

.PHONY: cleanall
cleanall:
	make clean
	rm -f $(patsubst %,%.pdf,$(target))

.PHONY: copy
copy:
#ifeq ($(whoami),manos)
#	rsync -a top.pdf <someshere>
#endif


