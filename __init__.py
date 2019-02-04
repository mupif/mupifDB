#
#               MuPIF: Multi-Physics Integration Framework 
#                   Copyright (C) 2010-2015 Borek Patzak
#
#       Czech Technical University, Faculty of Civil Engineering,
#       Department of Mechanics, 166 29 Prague, Czech Republic
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public
#  License along with this library; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

"""
This is a MuPIFDB module (Multi-Physics Integration Framework)
"""
#Major.Minor.Patch
__version__ = '0.0.1'
__author__  = 'Borek Patzak'

#List all submodules, so they can all be imported: from mupif import *
__all__ = ['workflowmanager']

from . import workflowmanager

