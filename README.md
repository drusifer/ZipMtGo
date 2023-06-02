# ZipMtGo
a tool that accellerates the compression of files using asych pool workers

# Usage
<pre>Usage of ZipMtGo.exe:
  -algo string
    	Must be one of [xz, bz2, gz] default is xz (default "xz")
  -input string
    	The input file name to compress.  Default is stdin
  -out string
    	The output file name to write the compressed output to. Use '-' for sdout. Default is input_file + suffix
  -t	Test the input file to make sure it is valid
</pre>
