
#!/bin/bash

#PBS -N STATSARBPCI
#PBS -l ncpus=2
#PBS -l mem=10GB
#PBS -J 1-3060
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif estimate_pci_padobran.R

