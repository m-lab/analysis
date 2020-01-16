package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"image"
	"image/color"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/fogleman/gg"
	"github.com/golang/freetype/truetype"
	"github.com/google/hilbert"
	"github.com/m-lab/go/rtx"
	"golang.org/x/image/font/gofont/goregular"
)

func speedToColor(s float64) color.RGBA {
	colors := map[float64]color.RGBA{
		1:           color.RGBA{0x76, 0xb8, 0xde, 0xff},
		4:           color.RGBA{0xa0, 0xbf, 0xd9, 0xff},
		10:          color.RGBA{0xc1, 0xd1, 0xe3, 0xff},
		20:          color.RGBA{0xff, 0xff, 0xff, 0xff},
		50:          color.RGBA{0xdb, 0xa6, 0x70, 0xff},
		100:         color.RGBA{0xd9, 0x83, 0x59, 0xff},
		math.Inf(1): color.RGBA{0xd6, 0x54, 0x40, 0xff},
	}
	for v, c := range colors {
		if s <= v {
			return c
		}
	}
	return colors[math.Inf(1)]
}

var (
	input  = flag.String("csv", "", "Name of the CSV file to read")
	output = flag.String("png", "", "Name of the PNG file to write")
	label  = flag.String("label", "", "The label to give this image")
)

func main() {
	flag.Parse()

	// Create a Hilbert curve for mapping to and from a 256*16 x 256*16 space
	s, err := hilbert.NewHilbert(256 * 16)
	rtx.Must(err, "Could not create space")

	im := image.NewRGBA(image.Rectangle{
		Min: image.Point{0, 0},
		Max: image.Point{256 * 16, 256 * 16},
	})
	for x := 0; x < 256*16; x++ {
		for y := 0; y < 256*16; y++ {
			im.SetRGBA(x, y, color.RGBA{40, 40, 40, 255})
		}
	}

	f, err := os.Open(*input)
	rtx.Must(err, "Could not open file %s", *input)
	r := csv.NewReader(f)

	for line, err := r.Read(); err == nil; line, err = r.Read() {
		block := line[0]
		ip := net.ParseIP(block)
		if ip == nil {
			continue
		}
		ip = ip.To4()
		d := int64(ip[0])*256*256 + int64(ip[1])*256 + int64(ip[2])
		speed, _ := strconv.ParseFloat(line[2], 64)

		x, y, err2 := s.Map(int(d))
		rtx.Must(err2, "Could not convert distance %d to xy", d)
		im.SetRGBA(int(x), int(y), speedToColor(speed))
	}
	if err != io.EOF {
		rtx.Must(err, "Error was not EOF")
	}

	// Now draw the /8 owners
	b, err := hilbert.NewHilbert(16)
	rtx.Must(err, "Could not create space")
	f, err = os.Open("slash8.csv")
	rtx.Must(err, "Could not open file")
	r = csv.NewReader(f)

	r.Read() // The header
	gi := gg.NewContextForRGBA(im)
	gi.SetRGBA(1, 1, 1, .5)
	font, err := truetype.Parse(goregular.TTF)
	gi.SetFontFace(truetype.NewFace(font, &truetype.Options{Size: 40}))
	for line, err := r.Read(); err == nil; line, err = r.Read() {
		ip, _, err := net.ParseCIDR(line[0])
		rtx.Must(err, "Could not parse block")
		ip = ip.To4()
		owner := strings.TrimSpace(line[1])
		d := int(ip[0])
		x, y, err := b.Map(d)
		rtx.Must(err, "Could not convert distance %d to xy", d)
		gi.DrawStringAnchored(owner, float64(x)*256+128, float64(y)*256+128, .5, .5)
		gi.DrawStringAnchored(fmt.Sprintf("%d", d), float64(x)*256+128, float64(y)*256+170, .5, .5)
	}
	if err != io.EOF {
		rtx.Must(err, "Error was not EOF")
	}
	gi.SetFontFace(truetype.NewFace(font, &truetype.Options{Size: 200}))
	gi.DrawStringAnchored(*label, 2048, 2048, .5, .5)

	rtx.Must(gi.SavePNG(*output), "Could not create image %s", *output)
}
