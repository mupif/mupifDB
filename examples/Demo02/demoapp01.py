from builtins import range
from mupif import *
import Pyro4
from mupifDB.examples.Demo02 import meshgen
import math
import numpy as np
import time as timeTime
import os
import logging
log = logging.getLogger('demoapp')

from pymongo import MongoClient
import argparse
import mupifDB.workflowmanager
from bson import ObjectId


import mupif.Physics.PhysicalQuantities as PQ
timeUnits = PQ.PhysicalUnit('s',   1.,    [0,0,1,0,0,0,0,0,0])

@Pyro4.expose
class thermal(Application.Application):
    """ Simple stationary heat transport solver on rectangular domains"""

    def __init__(self, file, workdir):
        super(thermal, self).__init__(file, workdir)
        self.morphologyType=None
        self.conductivity=Property.ConstantProperty(1, PropertyID.PID_effective_conductivity, ValueType.Scalar, 'W/m/K')
        self.dimx=Property.ConstantProperty(1, PropertyID.PID_Dimension, ValueType.Scalar, 'm')
        self.dimy=Property.ConstantProperty(1, PropertyID.PID_Dimension, ValueType.Scalar, 'm')
        self.BCS=[[None,0],[None,0],[None,0],[None, 0]]
    
        self.tria=False

    def setProperty(self, property, objectID=0):
        log.info('Setting property %s on %s'%(property.getPropertyID(), str(objectID)))
        if (property.getPropertyID() == PropertyID.PID_effective_conductivity):
            # remember the mapped value
            self.conductivity = property.inUnitsOf('W/m/K')
            #log.info("Assigning effective conductivity %f" % self.conductivity.getValue() )
        elif (property.getPropertyID() == PropertyID.PID_Dimension):
            if (objectID == 0):
                self.dimx = property.inUnitsOf('m')
            else:
                self.dimy = property.inUnitsOf('m')
        elif (property.getPropertyID() == PropertyID.PID_dirichletBC):
            self.BCS[objectID] = [property,0]
        elif (property.getPropertyID() == PropertyID.PID_conventionExternalTemperature):
            log.info("Assigning External teperature %f for edge %d" % (property.getValue(), objectID) )
            self.BCS[objectID][0] = property
        elif (property.getPropertyID() == PropertyID.PID_conventionCoefficient):
            self.BCS[objectID][1] = property
        else:
            raise APIError.APIError ('Unknown property ID')

    def getProperty(self, propID, time, objectID=0):
        if (propID == PropertyID.PID_effective_conductivity):
            #average reactions from solution - use nodes on edge 4 (coordinate x==0.)
            sumQ = 0.
            for i in range(self.mesh.getNumberOfVertices()):
                coord = (self.mesh.getVertex(i).getCoordinates())
                if coord[0] < 1.e-6:
                    ipneq = self.loc[i]
                    if ipneq>=self.neq:
                        #print (coord, ipneq)
                        #print (i,ipneq)
                        sumQ -= self.r[ipneq-self.neq]
            self.effConductivity = sumQ / self.yl * self.xl / (self.dirichletBCs[(self.ny+1)*(self.nx+1)-1] - self.dirichletBCs[0]   )
            #print (sumQ, self.effConductivity, self.dirichletBCs[(self.ny+1)*(self.nx+1)-1], self.dirichletBCs[0])
            return Property.ConstantProperty(self.effConductivity, PropertyID.PID_effective_conductivity, ValueType.Scalar, 'W/m/K', time, 0)
        else:
            raise APIError.APIError ('Unknown property ID')




    def setup(self, tria=False):
        self.tria = tria
        dirichletModelEdges=[]
        convectionModelEdges=[]
 
        self.xl=self.dimx.getValue()
        self.yl=self.dimy.getValue()
        log.info ("Thermal problem's dimensions: (%g, %g)" % (self.xl,self.yl) )

        self.nx=10
        self.ny=10

        for iedge in range(4):
            bc = self.BCS[iedge]
            if isinstance(bc[0], Property.Property):
                log.info('bc loop PID:%s'%bc[0].getPropertyID())
                if bc[0].getPropertyID() == PropertyID.PID_dirichletBC:
                    dirichletModelEdges.append((iedge+1,bc[0].getValue()))
                elif bc[0].getPropertyID() == PropertyID.PID_conventionExternalTemperature:
                    log.info('Detecting Convention BC on Edge %d '%iedge)
                    convectionModelEdges.append((iedge+1,bc[0].getValue(), bc[1].getValue()))
 
        #print (convectionModelEdges)

        self.mesh = Mesh.UnstructuredMesh()
        # generate a simple mesh here, either triangles or rectangles
        #self.xl = 0.5 # domain (0..xl)(0..yl)
        #self.yl = 0.3
        #self.nx = 10 # number of elements in x direction
        #self.ny = 10 # number of elements in y direction 
        self.dx = self.xl/self.nx;
        self.dy = self.yl/self.ny;
        self.mesh = meshgen.meshgen((0.,0.), (self.xl, self.yl), self.nx, self.ny, tria) 

#
# Model edges
#     ----------3----------
#     |                   |
#     4                   2
#     |                   | 
#     ----------1---------
#

        #dirichletModelEdges=(3,4,1)#
        self.dirichletBCs = {}# key is node number, value is prescribed temperature
        for (ide,value) in dirichletModelEdges:
            #print ("Dirichlet", ide)
            if ide == 1:
                for i in range(self.nx+1):
                    self.dirichletBCs[i*(self.ny+1)]=value
            elif ide == 2:
                for i in range(self.ny+1):
                    self.dirichletBCs[(self.ny+1)*(self.nx)+i]=value
            elif ide == 3:
                for i in range(self.nx+1):
                    self.dirichletBCs[self.ny + (self.ny+1)*(i)]=value
            elif ide == 4:
                for i in range(self.ny+1):
                    self.dirichletBCs[i]=value

        #convectionModelEdges=(2,)
        self.convectionBC = []
        for (ice, value, h) in convectionModelEdges:
            #print ("Convection", ice)
            if ice == 1:
                for i in range(self.nx):
                    if self.tria:
                        self.convectionBC.append((2*self.ny*i,0 , h, value))
                    else:
                        self.convectionBC.append((self.ny*i,0 , h, value))
            elif ice == 2:
                for i in range(self.ny):
                    if self.tria:
                        self.convectionBC.append(((self.nx-1)*2*self.ny+2*i, 1, h, value))
                    else:
                        self.convectionBC.append(((self.nx-1)*self.ny+i, 1, h, value))
            elif ice == 3:
                for i in range(self.nx):
                    if self.tria:
                        self.convectionBC.append((2*self.ny*(i+1)-1, 1, h, value))
                    else:
                        self.convectionBC.append((self.ny*(i+1)-1, 2, h, value))
            elif ice == 4:
                for i in range(self.ny):
                    if self.tria:
                        self.convectionBC.append((2*i+1, 2, h, value))
                    else:
                        self.convectionBC.append((i, 3, h, value))

        self.loc=np.zeros(self.mesh.getNumberOfVertices(), dtype=np.int32)
        self.neq = 0;#number of unknowns
        self.pneq = 0;#number of prescribed equations (Dirichlet b.c.)
        #print (self.mesh.getNumberOfVertices())
        for i in range(self.mesh.getNumberOfVertices()):
            #print(i)
            if i in self.dirichletBCs:
                self.pneq += 1
            else:
                self.neq += 1
        #print ("Neq", self.neq, "Pneq", self.pneq)
        #print(self.loc)
        ineq = 0 # unknowns numbering starts from 0..neq-1
        ipneq = self.neq #prescribed unknowns numbering starts neq..neq+pneq-1

        for i in range(self.mesh.getNumberOfVertices()):
            if i in self.dirichletBCs:
                self.loc[i] = ipneq
                ipneq += 1
            else:
                self.loc[i] = ineq
                ineq += 1
        #print (self.loc)
        self.setMetadata('Model.Model_ID','1')
        self.setMetadata('Model.Model_name','Thermal')
        self.setMetadata('Model.Model_description','Stationary heat conduction using finite elements on rectangular domain')
        self.setMetadata('Model.Model_material','Isotropic heat conducting material')
        self.setMetadata('Model.Model_type','Continuum')
        self.setMetadata('Model.Model_geometry','2D rectangle')
        self.setMetadata('Model.Model_time_lapse','seconds')
        self.setMetadata('Model.Model_manufacturing_service','Temperature')
        self.setMetadata('Model.Model_publication','Felippa: Introduction to finite element methods, 2004')
        self.setMetadata('Model.Model_entity',['Finite volume'])
        self.setMetadata('Model.Model_equation',['Heat balance'])
        self.setMetadata('Model.Model_equation_quantities',['Temperature','Heat-flow'])
        self.setMetadata('Model.Model_relation_formulation',['Flow-gradient'])
        self.setMetadata('Model.Model_relation_description ',['Conservation of energy'])
        self.setMetadata('Model.Model_numerical_solver','Finite element method')
        self.setMetadata('Model.Model_numerical_solver_additional_params','Time step, finite difference discretization of the time derivative')
        self.setMetadata('Model.Solver_name','Stationary thermal solver')
        self.setMetadata('Model.Solver_version_date','1.0, Dec 31 2018')
        self.setMetadata('Model.Solver_license','None')
        self.setMetadata('Model.Solver_creator','Borek Patzak')
        self.setMetadata('Model.Solver_language','Python')
        self.setMetadata('Model.Solver_time_step','seconds')
        self.setMetadata('Model.Model_computational_representation','Finite element')
        self.setMetadata('Model.Model_boundary_conditions','Dirichlet, Neumann')
        self.setMetadata('Model.Accuracy',0.75)
        self.setMetadata('Model.Sensitivity','Medium')
        self.setMetadata('Model.Complexity','Low')
        self.setMetadata('Model.Robustness','High')
        self.setMetadata('Model.Estimated_execution cost','0.01€')
        self.setMetadata('Model.Estimated_personnel cost','0.01€')
        self.setMetadata('Model.Required_expertise','User')
        self.setMetadata('Model.Estimated_computational_time','Seconds')
        self.setMetadata('Model.Required expertise','User')
        self.setMetadata('Model.Inputs_and_relation_to_Data',['Boundary temperature',1,'Scalar','','Ambient temperature on edges with heat convection'])
        self.setMetadata('Model.Outputs_and_relation_to_Data',['Temperature field',1,'Field','Resulting thermal field'])

        self.setMetadata('inputs', [{'name': 'Effective conductivity', 'type': 'Property', 'optional': False, 'obj_type': 'mupif.PropertyID.PID_effective_conductivity', 'units':'W/m/K', 'obj_id': None},
                                    {'name': 'Dimension', 'type': 'Property', 'optional': False,'obj_type': 'mupif.PropertyID.PID_Dimension', 'units':'m', 'obj_id': (0,1)},
                                    {'name': 'Prescribed temperature', 'type': 'Property', 'optional': True,'obj_type': 'mupif.PropertyID.PID_dirichletBC', 'units':'K', 'obj_id': (0,1,2,3)},
                                    {'name': 'External temperature', 'type': 'Property', 'optional': True,'obj_type': 'mupif.PropertyID.PID_conventionExternalTemperature', 'units':'K', 'obj_id': (0,1,2,3)},
                                    {'name': 'Convention coefficient', 'type': 'Property', 'optional': True,'obj_type': 'PID_conventionCoefficient', 'units':'none', 'obj_id': (0,1,2,3)}])
        self.setMetadata('outputs', [{'name':'Temperature field', 'type': 'Field', 'optional':True,'obj_type':'mupif.FieldID.FID_Temperature', 'units':'T', 'obj_id': None}])

    def getField(self, fieldID, time, objectID=0):
        if (fieldID == FieldID.FID_Temperature):
            values=[]
            for i in range (self.mesh.getNumberOfVertices()):
                if time.getValue()==0.0:#put zeros everywhere
                    values.append((0.,))
                else:
                    values.append((self.T[self.loc[i]],))
            return Field.Field(self.mesh, FieldID.FID_Temperature, ValueType.Scalar, 'C', time, values);
        elif (fieldID == FieldID.FID_Material_number):
            values=[]
            for e in self.mesh.cells():
                if self.isInclusion(e) and self.morphologyType=='Inclusion':
                    values.append((1,))
                else:
                    values.append((0,))
            #print (values)
            return Field.Field(self.mesh, FieldID.FID_Material_number, ValueType.Scalar, PQ.getDimensionlessUnit(), time, values,fieldType=Field.FieldType.FT_cellBased);
        else:
            raise APIError.APIError ('Unknown field ID')

    def isInclusion(self,e):
        vertices = e.getVertices()
        c1=vertices[0].coords
        c2=vertices[1].coords
        c3=vertices[2].coords
        c4=vertices[3].coords
        xCell = (c1[0]+c2[0]+c3[0]+c4[0])/4. #vertex center
        yCell = (c1[1]+c2[1]+c3[1]+c4[1])/4. #vertex center
        radius = min(self.xl, self.yl) * self.scaleInclusion
        xCenter = self.xl/2.#domain center
        yCenter = self.yl/2.#domain center
        if ( math.sqrt((xCell-xCenter)*(xCell-xCenter) + (yCell-yCenter)*(yCell-yCenter)) < radius ):
            return True
            #print (xCell,yCell)
        return False

    def setField(self, field):
        self.Field = field

    def solveStep(self, tstep, stageID=0, runInBackground=False):
        self.setup()
        mesh = self.mesh
        self.volume = 0.0;
        self.integral = 0.0;

        numNodes = mesh.getNumberOfVertices()
        numElements= mesh.getNumberOfCells()
        ndofs = 4

        #print numNodes
        #print numElements
        #print ndofs

        start = timeTime.time()
        log.info(self.getApplicationSignature())
        log.info("Number of equations: %d" % self.neq)

        #connectivity 
        c=np.zeros((numElements,4), dtype=np.int32)
        for e in range(0,numElements):
            for i in range(0,4):
                c[e,i]=self.mesh.getVertex(mesh.getCell(e).vertices[i]).label
        #print "connectivity :",c

        #Global matrix and global vector
        kuu = np.zeros((self.neq,self.neq))
        kpp = np.zeros((self.pneq,self.pneq))
        kup = np.zeros((self.neq,self.pneq))
        #A = np.zeros((self.neq, self.neq ))
        b = np.zeros(self.neq)
        # solution vector
        self.T = np.zeros(self.neq+self.pneq) #vector of temperatures

        #initialize prescribed Temperatures in current solution vector (T):
        for i in range(self.mesh.getNumberOfVertices()):
            if i in self.dirichletBCs:
                ii = self.loc[i] 
                self.T[ii] = self.dirichletBCs[i] #assign temperature

        log.info("Assembling ...")
        for e in mesh.cells():
            A_e = self.compute_elem_conductivity(e, self.conductivity.getValue(tstep.getTime()))

            # #Assemble
            #print e, self.loc[c[e.number-1,0]],self.loc[c[e.number-1,1]], self.loc[c[e.number-1,2]], self.loc[c[e.number-1,3]] 
            for i in range(ndofs):#loop of dofs
                ii = self.loc[c[e.number-1,i]]#code number
                if ii<self.neq:#unknown to be solved
                    for j in range(ndofs):
                        jj = self.loc[c[e.number-1,j]]
                        if jj<self.neq:
                            kuu[ii,jj] += A_e[i,j]
                        else:
                            kup[ii,jj-self.neq] += A_e[i,j]
                else:#prescribed value
                    for j in range(ndofs):
                        jj = self.loc[c[e.number-1,j]]
                        if jj>=self.neq:
                            kpp[ii-self.neq,jj-self.neq] += A_e[i,j]

        #print (A)
        #print (b)

        # add boundary terms
        #print ('Convection BC', self.convectionBC)
        for i in self.convectionBC:
            #print "Processing bc:", i
            elem = mesh.getCell(i[0])
            side = i[1]
            h = i[2]
            Te = i[3]
            #print ("h:%f Te:%f" % (h, Te))

            n1 = elem.getVertices()[side];
            #print n1
            if (side == 3):
                n2 = elem.getVertices()[0]
            else:
                n2 = elem.getVertices()[side+1]

            length = math.sqrt((n2.coords[0]-n1.coords[0])*(n2.coords[0]-n1.coords[0]) +
                               (n2.coords[1]-n1.coords[1])*(n2.coords[1]-n1.coords[1]))

            #print h, Te, length

            # boundary_lhs=h*(np.dot(N.T,N))
            boundary_lhs=np.zeros((2,2))
            if self.tria:
                boundary_lhs[0,0] = h*(1./4.)*length
                boundary_lhs[0,1] = h*(1./4.)*length
                boundary_lhs[1,0] = h*(1./4.)*length
                boundary_lhs[1,1] = h*(1./4.)*length
            else:
                boundary_lhs[0,0] = h*(1./3.)*length
                boundary_lhs[0,1] = h*(1./6.)*length
                boundary_lhs[1,0] = h*(1./6.)*length
                boundary_lhs[1,1] = h*(1./3.)*length

            # boundary_rhs=h*Te*N.T
            boundary_rhs = np.zeros((2,1))
            boundary_rhs[0] = h*(1./2.)*length*Te
            boundary_rhs[1] = h*(1./2.)*length*Te

            # #Assemble
            loci = [n1.number, n2.number]
            #print loci
            for i in range(2):#loop nb of dofs
                ii = self.loc[loci[i]]
                if ii<self.neq:
                    for j in range(2):
                        jj = self.loc[loci[j]]
                        if jj<self.neq:
                            #print "Assembling bc ", ii, jj, boundary_lhs[i,j]
                            kuu[ii,jj] += boundary_lhs[i,j]
                    b[ii] += boundary_rhs[i] 

        self.r = np.zeros(self.pneq)#reactions

        #solve linear system
        log.info("Solving thermal problem")
        #self.rhs = np.zeros(self.neq)
        self.rhs = b - np.dot(kup,self.T[self.neq:self.neq+self.pneq])
        self.T[:self.neq] = np.linalg.solve(kuu,self.rhs)
        self.r = np.dot(kup.transpose(),self.T[:self.neq])+np.dot(kpp,self.T[self.neq:self.neq+self.pneq])
        #print (self.r)

        log.info("Done")
        log.info("Time consumed %f s" % (timeTime.time()-start))


    def compute_B(self, elem, lc):
        # computes gradients of shape functions of given element
        vertices = elem.getVertices()
        
        if isinstance(elem, Cell.Quad_2d_lin):
            c1=vertices[0].coords
            c2=vertices[1].coords
            c3=vertices[2].coords
            c4=vertices[3].coords

            B11=0.25*(c1[0]-c2[0]-c3[0]+c4[0])
            B12=0.25*(c1[0]+c2[0]-c3[0]-c4[0])
            B21=0.25*(c1[1]-c2[1]-c3[1]+c4[1])
            B22=0.25*(c1[1]+c2[1]-c3[1]-c4[1])

            C11=0.25*(c1[0]-c2[0]+c3[0]-c4[0])
            C12=0.25*(c1[0]-c2[0]+c3[0]-c4[0])
            C21=0.25*(c1[1]-c2[1]+c3[1]-c4[1])
            C22=0.25*(c1[1]-c2[1]+c3[1]-c4[1])

            #local coords
            ksi=lc[0]
            eta=lc[1]

            B = np.zeros((2,2))
            B[0,0] = (1./elem.getTransformationJacobian(lc))*(B22+ksi*C22)  
            B[0,1] = (1./elem.getTransformationJacobian(lc))*(-B21-eta*C21) 
            B[1,0] = (1./elem.getTransformationJacobian(lc))*(-B12-ksi*C12) 
            B[1,1] = (1./elem.getTransformationJacobian(lc))*(B11+eta*C11) 

            dNdksi = np.zeros((2,4))
            dNdksi[0,0] = 0.25 * ( 1. + eta )
            dNdksi[0,1] = -0.25 * ( 1. + eta )
            dNdksi[0,2] = -0.25 * ( 1. - eta )
            dNdksi[0,3] = 0.25 * ( 1. - eta )
            dNdksi[1,0] = 0.25 * ( 1. + ksi )
            dNdksi[1,1] = 0.25 * ( 1. - ksi )
            dNdksi[1,2] = -0.25 * ( 1. - ksi )
            dNdksi[1,3] = -0.25 * ( 1. + ksi )

            Grad = np.zeros((2,4))
        elif isinstance(elem, Cell.Triangle_2d_lin):
            c1=vertices[0].coords
            c2=vertices[1].coords
            c3=vertices[2].coords
            #local coords
            ksi=lc[0]
            eta=lc[1]
            B = np.zeros((2,2))
            B[0,0] = (1./elem.getTransformationJacobian(lc))*(c2[1]-c3[1])  
            B[0,1] = (1./elem.getTransformationJacobian(lc))*(-c1[1]+c3[1]) 
            B[1,0] = (1./elem.getTransformationJacobian(lc))*(-c2[0]+c3[0]) 
            B[1,1] = (1./elem.getTransformationJacobian(lc))*(c1[0]-c3[0]) 
            dNdksi = np.zeros((2,3))
            dNdksi[0,0] = 1#N1=ksi, N2=eta, N3=1-ksi-eta
            dNdksi[0,1] = 0
            dNdksi[0,2] = -1
            dNdksi[1,0] = 0
            dNdksi[1,1] = 1
            dNdksi[1,2] = -1
            Grad = np.zeros((2,4))
            
        Grad = np.dot(B,dNdksi)
        #print Grad
        return Grad

    def compute_elem_conductivity (self, e, k):
        #compute element conductivity matrix
        numVert = e.getNumberOfVertices()
        A_e = np.zeros((numVert,numVert))
        b_e = np.zeros((numVert,1))
        rule = IntegrationRule.GaussIntegrationRule()

        ngp  = rule.getRequiredNumberOfPoints(e.getGeometryType(), 2)
        pnts = rule.getIntegrationPoints(e.getGeometryType(), ngp)

        #print "e : ",e.number-1
        #print "ngp :",ngp
        #print "pnts :",pnts

        for p in pnts: # loop over ips
            detJ=e.getTransformationJacobian(p[0])
            #print "Jacobian: ",detJ

            dv = detJ * p[1]
            #print "dv :",dv 

            N = np.zeros((1,numVert)) 
            tmp = e._evalN(p[0]) 
            N=np.asarray(tmp)
            #print "N :",N

            x = e.loc2glob(p[0])
            #print "global coords :", x

            #conductivity
            #k=self.conductivity.getValue()
            if self.morphologyType=='Inclusion':
                if self.isInclusion(e):
                    k=0.001

            Grad= np.zeros((2,numVert))
            Grad = self.compute_B(e,p[0])
            #print "Grad :",Grad
            K=np.zeros((numVert,numVert))
            K=k*dv*(np.dot(Grad.T,Grad))
                
            #Conductivity matrix
            for i in range(numVert):#loop dofs
                for j in range(numVert):
                    A_e[i,j] += K[i,j]
        return A_e


    def getCriticalTimeStep(self):
        return PQ.PhysicalQuantity(1.0, 's')

    def getAssemblyTime(self, tstep):
        return tstep.getTime()

    def getApplicationSignature(self):
        return "Stationary thermal-demo-solver, ver 1.0"




if __name__ == "__main__":
    # execute only if run as a script
    client = MongoClient()
    db = client.MuPIF
    parser = argparse.ArgumentParser()
    parser.add_argument('-eid', '--executionID', required=True, dest="id")
    args = parser.parse_args()
    print ('WEID:', args.id)
    wec = mupifDB.workflowmanager.WorkflowExecutionContext(db, ObjectId(args.id))
    inp = wec.getIODataDoc('Inputs')
    print (inp)

    log.info(inp.get('Effective conductivity', None))
    log.info(inp.get('External temperature', obj_id=0))

    app = thermal(None, None)
    mupifDB.workflowmanager.mapInputs(app, db, args.id)

    tstep = TimeStep.TimeStep(1.,1.,10,'s')



    app.solveStep(tstep)
    app.terminate()

    f = app.getField(FieldID.FID_Temperature, tstep.getTargetTime())
    f.field2VTKData().tofile('temperature')