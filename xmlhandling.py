#!/usr/bin/python

import xml.etree.ElementTree as ET

class xmlparsing:
    xmlfile = None
    tree = None
    root = None
    def __init__(self,filename):
        self.xmlfile = filename
        self.tree = ET.parse(filename)
        self.root = self.tree.getroot()


    def createProperty(self,pname,pvalue,pdesc):
        self.property = ET.Element ('property')
        name = ET.SubElement(self.property,'name')
        value = ET.SubElement(self.property,'value')
        description = ET.SubElement(self.property,'description')
        name.text = pname
        value.text = pvalue
        description.text = pdesc
        return self.property

    def appendProperty(self,pname,pvalue,pdesc='No description'):
        self.root.append(self.createProperty(pname,pvalue,pdesc))


    def saveFile(self):
        self.tree.write(self.xmlfile)
