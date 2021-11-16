/*
 * Copyright 2013-2020, Université de Nantes, CNRS, INSERM, l’institut du thorax, F-44000 Nantes, France.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.dag

import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.text.SimpleDateFormat
/**
 * Render the DAG in as DAX (pegasus)
 * See https://gephi.org/ for more info.
 *
 * @author Robin Gögge / rgo3
 */
class DaxRenderer implements DagRenderer {
    private static final String VERSION="3.6"
    private static final String XMLNS="http://pegasus.isi.edu/schema/DAX"
    private static final String XSI_SCHEMA_LOCATION="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd"


    private final String name

    DaxRenderer(String name) {
        this.name = name
    }

    @Override
    void renderDocument(DAG dag, Path file) {
        final Charset charset = Charset.defaultCharset()
        Writer bw = Files.newBufferedWriter(file, charset)
        final XMLOutputFactory xof = XMLOutputFactory.newFactory()
        final XMLStreamWriter w = xof.createXMLStreamWriter(bw)
        w.writeStartDocument(charset.displayName(),"1.0")
        w.writeStartElement("adag")
        w.writeAttribute("xmlns",XMLNS)
        w.writeAttribute("xmlns:xsi","http://www.w3.org/2001/XMLSchema-instance")
        w.writeAttribute("xsi:schemaLocation",XSI_SCHEMA_LOCATION)
        w.writeAttribute("version", VERSION)

        /* vertex/node/job */
        dag.vertices.each { vertex -> renderVertex(w, vertex ) }

        /* edges */
        dag.edges.each { edge -> renderEdge(w, edge ) }

        w.writeEndElement()
        w.writeEndDocument()
        w.flush()
        bw.flush()
        bw.close()
    }

    private void renderVertex(w,vertex) {
        /* 
        <job id="ID00000" namespace="Genome" name="fastqSplit_chr21" version="1.0" runtime="35.79">
            <uses file="chr210.sfq" link="input" register="true" transfer="true" optional="false" type="data" size="249228055"/>
            <uses file="chr21.0.0.sfq" link="output" register="true" transfer="true" optional="false" type="data" size="30755085"/>
        </job> */
        w.writeStartElement("job")
        w.writeAttribute("id",vertex.getName())
        w.writeAttribute("name",vertex.label?vertex.label:vertex.getName())
        w.writeAttribute("runtime","tbd")
        vertex.getProcess().getConfig().getInputs().each { inP -> renderInput(w, inP ) }
        vertex.getProcess().getConfig().getOutputs().each { outP -> renderOutput(w, outP) }
        w.writeEndElement() //job
    }

    private void renderInput(w,input) {
        w.writeStartElement("uses")
        w.writeAttribute("file",input.getName())
        w.writeAttribute("link","input")
        w.writeAttribute("register","true")
        w.writeAttribute("transfer","true")
        w.writeAttribute("optional","false")
        w.writeAttribute("type","data")
        w.writeAttribute("size","tbd")
        w.writeEndElement() //uses
    }

    private void renderOutput(w,output) {
        w.writeStartElement("uses")
        w.writeAttribute("file",output.getName())
        w.writeAttribute("link","output")
        w.writeAttribute("register","true")
        w.writeAttribute("transfer","true")
        w.writeAttribute("optional","false")
        w.writeAttribute("type","data")
        w.writeAttribute("size","tbd")
        w.writeEndElement() //uses
    }

    private void renderEdge(w,edge) {
        /* 
        <child ref="ID00001">
            <parent ref="ID00000"/>
        </child> */
        assert edge.from != null && edge.to != null
        w.writeStartElement("child")
        w.writeAttribute("ref", edge.to.name)
        w.writeStartElement("parent")
        w.writeAttribute("ref", edge.from.name)
        w.writeEndElement() //parent
        w.writeEndElement() //child
    }
}
