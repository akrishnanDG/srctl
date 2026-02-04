package output

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/yaml.v3"
)

// Format represents the output format
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
	FormatYAML  Format = "yaml"
	FormatPlain Format = "plain"
)

// Printer handles formatted output
type Printer struct {
	format Format
}

// NewPrinter creates a new printer with the specified format
func NewPrinter(format string) *Printer {
	f := Format(strings.ToLower(format))
	switch f {
	case FormatTable, FormatJSON, FormatYAML, FormatPlain:
		return &Printer{format: f}
	default:
		return &Printer{format: FormatTable}
	}
}

// Print outputs data in the configured format
func (p *Printer) Print(data interface{}) error {
	switch p.format {
	case FormatJSON:
		return p.printJSON(data)
	case FormatYAML:
		return p.printYAML(data)
	case FormatPlain:
		return p.printPlain(data)
	default:
		return p.printTable(data)
	}
}

func (p *Printer) printJSON(data interface{}) error {
	output, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func (p *Printer) printYAML(data interface{}) error {
	output, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	fmt.Print(string(output))
	return nil
}

func (p *Printer) printPlain(data interface{}) error {
	switch v := data.(type) {
	case []string:
		for _, s := range v {
			fmt.Println(s)
		}
	case []int:
		for _, i := range v {
			fmt.Println(i)
		}
	case string:
		fmt.Println(v)
	default:
		fmt.Printf("%v\n", v)
	}
	return nil
}

func (p *Printer) printTable(data interface{}) error {
	// This is a generic table printer, specific commands may implement their own
	switch v := data.(type) {
	case []string:
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Value"})
		table.SetBorder(false)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		for _, s := range v {
			table.Append([]string{s})
		}
		table.Render()
	case [][]string:
		if len(v) == 0 {
			return nil
		}
		table := tablewriter.NewWriter(os.Stdout)
		if len(v) > 0 {
			table.SetHeader(v[0])
			for _, row := range v[1:] {
				table.Append(row)
			}
		}
		table.SetBorder(false)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.Render()
	default:
		return p.printJSON(data)
	}
	return nil
}

// PrintTable prints a table with headers
func PrintTable(headers []string, rows [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.SetBorder(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(true)
	table.SetAutoWrapText(false)
	for _, row := range rows {
		table.Append(row)
	}
	table.Render()
}

// Colors for terminal output
var (
	Green   = color.New(color.FgGreen).SprintFunc()
	Red     = color.New(color.FgRed).SprintFunc()
	Yellow  = color.New(color.FgYellow).SprintFunc()
	Blue    = color.New(color.FgBlue).SprintFunc()
	Cyan    = color.New(color.FgCyan).SprintFunc()
	Magenta = color.New(color.FgMagenta).SprintFunc()
	Bold    = color.New(color.Bold).SprintFunc()
)

// Success prints a success message
func Success(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", Green("✓"), fmt.Sprintf(format, args...))
}

// Error prints an error message
func Error(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", Red("✗"), fmt.Sprintf(format, args...))
}

// Warning prints a warning message
func Warning(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", Yellow("⚠"), fmt.Sprintf(format, args...))
}

// Info prints an info message
func Info(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", Blue("ℹ"), fmt.Sprintf(format, args...))
}

// Step prints a step message
func Step(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", Cyan("→"), fmt.Sprintf(format, args...))
}

// Header prints a header
func Header(format string, args ...interface{}) {
	fmt.Printf("\n%s\n", Bold(fmt.Sprintf(format, args...)))
	fmt.Println(strings.Repeat("─", 50))
}

// SubHeader prints a sub-header
func SubHeader(format string, args ...interface{}) {
	fmt.Printf("\n%s\n", Cyan(fmt.Sprintf(format, args...)))
}

// FormatBytes formats bytes to human readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

