# Copyright 2001-2011 by 
# Broadmind Research Corporation

SEPARATOR = :
JAVA = java
JAVAC = javac
#JAVAC = javac -deprecation 
JAVAH = javah
JAR = jar
JAVADOC = javadoc
SRCDIR = src/com/broadmind/xdb
BINDIR = bin
LIBDIR = lib
DOCDIR = doc
RESDIR = res
CPATH = $(BINDIR)
EDIRS = ./$(SEPARATOR)$(LIBDIR)
RM = rm -Rf
RUN = Main
CP = cp -Rp

all:
	$(JAVAC) -cp *:. -Djava.ext.dirs=./ -d $(BINDIR) $(SRCDIR)/*.java
	$(CP) $(RESDIR)/* $(BINDIR)/.

world: all jdoc jar

remake: clean all

#test: all
test:
	$(JAVAC) -cp $(CPATH):*:. -Djava.ext.dirs=./ ./*.java

jar:
	$(JAR) cvf $(LIBDIR)/xdb.jar -C $(BINDIR) .

jdoc:
	$(JAVADOC) -d $(DOCDIR) $(SRCDIR)/*.java

export:
	$(CP) $(LIBDIR)/* ../xcheck/WebContent/WEB-INF/lib/.
	$(CP) $(LIBDIR)/* /webapps/localhost/WEB-INF/lib/.

clean:
	$(RM) $(BINDIR)/*
	$(RM) $(LIBDIR)/*
	$(RM) $(DOCDIR)/*

run: test
	#$(JAVA) -cp ./ -Djava.ext.dirs=$(EDIRS) $(RUN)
	$(JAVA) -cp ./$(SEPARATOR)$(BINDIR) -Djava.ext.dirs=./ $(RUN)

wrun: test
	$(JAVA) -cp ./ -Djava.ext.dirs=./ $(RUN) 

