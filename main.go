package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/andykillmer/go-dcraw-json"
	"github.com/jbuchbinder/gopnm"
	"github.com/jeffail/tunny"
	"github.com/nfnt/resize"
	"github.com/pkg/profile"
	"golang.org/x/image/tiff"
	"image"
	"image/jpeg"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

var (
	dcrawPath    string
	previewWidth uint
	thumbWidth   uint
	debug        bool
)

type Task struct {
	Id         int    `json:"id"`
	Filename   string `json:"filename"`
	ImageWidth uint   `json:"imageWidth"`
	ThumbWidth uint   `json:"thumbWidth"`
}

type Resp struct {
	Preview   string `json:"preview"`
	Thumbnail string `json:"thumbnail"`
}

type TaskResult struct {
	Id       int    `json:"id"`
	Error    string `json:"error"`
	Response Resp   `json:"response"`
}

func main() {
	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs)

	cmdPath := strings.TrimSuffix(os.Args[0], "imaging") + "dcraw-json"

	flag.StringVar(&dcrawPath, "dcraw", cmdPath, "path to dcraw-json program")
	flag.UintVar(&previewWidth, "previewWidth", 1200, "preview image width")
	flag.UintVar(&thumbWidth, "thumbWidth", 400, "thumbnail image width")
	flag.BoolVar(&debug, "debug", true, "enable debug mode")
	flag.Parse()

	if debug {
		defer profile.Start(profile.MemProfile, profile.ProfilePath("./profiling/")).Stop()
	}

	if err := dcraw.Path(dcrawPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// setup the worker pool
	pool, _ := tunny.CreatePool(numCPUs, func(object interface{}) interface{} {
		task, _ := object.(Task)
		return resizeImage(task)
	}).Open()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Bytes()
		t := Task{}
		if err := json.Unmarshal(input, &t); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to unmarshal task: %s\n", err)
			continue
		}

		go func() {
			resp, err := pool.SendWork(t)
			if err != nil {
				r := TaskResult{}
				r.Id = t.Id
				r.Error = fmt.Sprintf("Failed to send work to pool: %s", err)
				printResult(r)
			} else {
				printResult(resp.(TaskResult))
			}
		}()
	}
}

func printResult(r TaskResult) {

	rBytes, err := json.Marshal(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not unmarshal task result: %+v", r)
	}

	rString := string(rBytes)
	if r.Error != "" {
		fmt.Fprintln(os.Stderr, rString)
	} else {
		fmt.Fprintln(os.Stdout, rString)
	}
}

//func resizeImage(t *Task, wg *sync.WaitGroup) {
func resizeImage(t Task) TaskResult {
	// all of the needed vars are declared here, since goto is used a lot
	var (
		sourceImageFile  *os.File
		previewImageFile *os.File
		thumbImageFile   *os.File
		sourceImage      image.Image
		previewImage     image.Image
		thumbImage       image.Image
		resp             TaskResult
	)

	resp.Id = t.Id

	halfSize := t.ImageWidth / 2
	// the rest to be filled out below
	args := []string{"-c"}

	// the preview image is going to be the source image for the thumbnail
	// extract or decode the largest and nearest size
	if t.ThumbWidth >= previewWidth && t.ThumbWidth <= halfSize {
		// embedded thumbnails can be full res, only opt for this if the embedded thumbnail
		// is smaller than the -h option (less memory needed)
		args = append(args, "-e")
	} else if halfSize >= previewWidth {
		// use the half size option for dcraw
		args = append(args, []string{"-w", "-h", "-T"}...)
		// camera's white balance, half size, TIFF output
	} else if t.ThumbWidth >= previewWidth {
		// the camera's embedded thumbnail is now preferred to the full res,
		// since the camera generated this image
		args = append(args, "-e")
	} else {
		// finally, the only option is the full resolution image
		args = append(args, []string{"-w", "-T"}...)
		// camera white balance, TIFF output
	}
	args = append(args, t.Filename)

	sourceImageFile, err := ioutil.TempFile("", "")
	if err != nil {
		resp.Error = err.Error()
		return resp
	}

	if err := dcraw.Run(args, sourceImageFile); err == nil {
		// dcraw successfully decoded the image, prepare it for reading
		sourceImageFile.Sync()
		sourceImageFile.Seek(0, 0)
		defer os.Remove(sourceImageFile.Name())
	} else {
		// determine if the file exists (it may have changed while in queue)
		if _, err := os.Stat(t.Filename); os.IsNotExist(err) {
			resp.Error = "File does not exist"
			return resp
		}
		// dcraw could not decode the image, but maybe its already a JPEG or similar
		os.Remove(sourceImageFile.Name()) // no longer needed (an empty file)
		sourceImageFile, _ = os.Open(t.Filename)
		defer sourceImageFile.Close()
	}

	// now sourceImageFile has the image we want to use for resizing
	sourceImage, err = decodeImage(sourceImageFile)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}

	// at this point, sourceImage is ready to resize, prepare the preview/thumb files
	previewImageFile, err = ioutil.TempFile("", "")
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	thumbImageFile, err = ioutil.TempFile("", "")
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	// do the resizing in this sequence
	previewImage = resize.Resize(previewWidth, 0, sourceImage, resize.Bilinear)
	thumbImage = resize.Resize(thumbWidth, 0, previewImage, resize.NearestNeighbor)
	// encode the two images to disk
	if err := jpeg.Encode(previewImageFile, previewImage, nil); err != nil {
		// remove the two temp image files
		os.Remove(previewImageFile.Name())
		os.Remove(thumbImageFile.Name())
		resp.Error = err.Error()
		return resp
	}
	if err := jpeg.Encode(thumbImageFile, thumbImage, nil); err != nil {
		os.Remove(thumbImageFile.Name())
		resp.Error = err.Error()
		return resp
	}
	// got this far? success!
	resp.Response.Preview = previewImageFile.Name()
	resp.Response.Thumbnail = thumbImageFile.Name()
	previewImageFile.Close()
	thumbImageFile.Close()

	if debug {
		defer os.Remove(previewImageFile.Name())
		defer os.Remove(thumbImageFile.Name())
	}

	return resp
}

func decodeImage(f *os.File) (image.Image, error) {
	if result, err := jpeg.Decode(f); err == nil {
		return result, nil
	}

	if result, err := tiff.Decode(f); err == nil {
		return result, nil
	}

	if result, err := pnm.Decode(f); err == nil {
		return result, nil
	}

	return nil, fmt.Errorf("Could not decode image (not jpeg/tiff/pnm)")
}
